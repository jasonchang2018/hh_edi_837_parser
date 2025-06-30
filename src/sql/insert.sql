insert into
    edwprodhh.edi_837_parser.response
select      sha2(METADATA$FILENAME)                                             as response_id,
            $1                                                                  as response_body,
            row_number() over (partition by METADATA$FILENAME order by seq)     as response_body_index,
            METADATA$FILENAME                                                   as file_name,
            current_date()                                                      as upload_date
from        (
                select      $1,
                            metadata$filename,
                            seq4() as seq
                -- from        @edwprodhh.edi_837_parser.stg_response/xl212927fw_250426_i5.837
                from        @edwprodhh.edi_837_parser.stg_response
                            (file_format => edwprodhh.edi_837_parser.format_txt)
            )
where       METADATA$FILENAME not in (select file_name from edwprodhh.edi_837_parser.response_files)
;

insert into
    edwprodhh.edi_837_parser.response_files (file_name)
select      distinct
            file_name
from        edwprodhh.edi_837_parser.response
where       upload_date = current_date()
;