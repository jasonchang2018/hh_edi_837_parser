-- Raw 837 files:   \\hh-fileserver01\TempUL2\IU_Health_Complex\837_FILES_IN\2025
-- Sample UB files: T:\Complex Claims\IU\PATIENT FOLDERS


-- Add raw 837 files to stage.
-- snowsql -q "PUT file://\\\\hh-fileserver01\\TempUL2\\IU_Health_Complex\\837_FILES_IN\\2025\\*.837 @edwprodhh.edi_837_parser.stg_response auto_compress=false;"
-- list @edwprodhh.edi_837_parser.stg_response;

create or replace procedure
    edwprodhh.hermes.insert_837_from_stage(EXECUTE_TIME TIMESTAMP_LTZ(9))
returns     boolean
language    sql
as
begin

    insert into
        edwprodhh.edi_837_parser.response
    select      sha2(METADATA$FILENAME)                                             as response_id,
                $1                                                                  as response_body,
                row_number() over (partition by METADATA$FILENAME order by seq)     as line_number,
                METADATA$FILENAME                                                   as file_name,
                :execute_time::date                                                 as upload_date
    from        (
                    select      $1,
                                metadata$filename,
                                seq4() as seq
                    from        @edwprodhh.edi_837_parser.stg_response
                                (file_format => edwprodhh.edi_837_parser.format_txt)
                    where       METADATA$FILENAME not in (select file_name from edwprodhh.edi_837_parser.response_files)
                )
    ;

    insert into
        edwprodhh.edi_837_parser.response_files (file_name)
    select      distinct
                file_name
    from        edwprodhh.edi_837_parser.response
    where       upload_date = current_date()
    ;

end
;



create or replace task
    edwprodhh.pub_jchang.sp_insert_837_from_stage
    warehouse = analysis_wh
    schedule = 'USING CRON 0 1 * * * America/Chicago'
as
call    edwprodhh.hermes.insert_837_from_stage(current_timestamp())
;