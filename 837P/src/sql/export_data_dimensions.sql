create table
    edwprodhh.edi_837p_parser.export_data_dimensions_log
(
    LINE_ELEMENT_837    VARCHAR(16777216),
    RESPONSE_ID         VARCHAR(16777216),
    INDEX               NUMBER(38,0),
    PL_GROUP            VARCHAR(16777216),
    CLAIM_ID            VARCHAR(16777216),
    FINAL_RN            NUMBER(18,0),
    UPLOAD_DATE         DATE
)
;


create or replace task
    edwprodhh.edi_837p_parser.insert_export_data_dimensions_log
    warehouse = analysis_wh
    after edwprodhh.edi_837p_parser.insert_export_data_dimensions_accounts_log
as
insert into
    edwprodhh.edi_837p_parser.export_data_dimensions_log
with claims as
(
    with formatted as
    (
        with file_dates as
        (
            select      distinct
                        response_id,
                        file_date
            from        edwprodhh.edi_837p_parser.response_flat
        )
        select      claims.response_id,
                    claims.nth_transaction_set,
                    claims.index,
                    claims.claim_index,
                    ltrim(claims.clm_ref_medical_record_num, '0')                       as mrn_,
                    regexp_substr(ltrim(claims.claim_id, '0'), '(\\d*$)', 1, 1, 'e')    as claim_id_
                    
        from        edwprodhh.edi_837p_parser.claims as claims
                    inner join
                        file_dates
                        on claims.response_id = file_dates.response_id
        qualify     row_number() over ( partition by    mrn_,
                                                        claim_id_
                                        order by        file_dates.file_date    desc,
                                                        claims.claim_index      desc)   = 1
    )
    select      formatted.*,
                debtor.pl_group
    from        formatted
                inner join
                    edwprodhh.edi_837p_parser.export_data_dimensions_accounts as debtor
                    on formatted.claim_id_ = debtor.cdn
)
, response_lines as
(
    select      response.response_id,
                response.index,
                response.nth_transaction_set,
                response.line_element_837 || '~' as line_element_837,
                claims.claim_id_ as claim_id,
                claims.pl_group
    from        edwprodhh.edi_837p_parser.response_flat as response
                inner join
                    claims
                    on  response.response_id            = claims.response_id
                    and response.nth_transaction_set    = claims.nth_transaction_set
    order by    1,2
)
, headers as
(
    with header_lines as
    (
        select      'ISA*00*          *00*          *ZZ*' || rpad('580977458', 15, ' ') || '*ZZ*' || rpad('12345678', 15, ' ') || '*' || to_varchar(current_timestamp(), 'yymmdd*hh24mi') || '*^*00501*000000001*1*P*:~'    as line_element_837,
                    -2                                                                                                                                                                                                      as index
        union all
        select      'GS*HC*580977458*12345678*' || to_varchar(current_timestamp(), 'yyyymmdd*hh24mi') || '*000000001*X*005010X222A1~'                                                                                       as line_element_837,
                    -1                                                                                                                                                                                                      as index
    )
    , pl_groups as
    (
        select      distinct
                    pl_group
        from        response_lines
    )
    select      header_lines.line_element_837,
                header_lines.index,
                pl_groups.pl_group,
                NULL as claim_id
    from        header_lines
                cross join
                    pl_groups
)
, unioned as
(
    select      line_element_837,
                response_id,
                index,
                pl_group,
                claim_id
    from        response_lines
    union all
    select      line_element_837,
                NULL as response_id,
                index,
                pl_group,
                claim_id
    from        headers
)
select      *,
            row_number() over (order by     pl_group,
                                            case when response_id is null then 1 else 2 end asc, --headers first
                                            response_id,
                                            index)                                                                  as final_rn,
            current_date()                                                                                          as upload_date
from        unioned
order by    final_rn
;


create or replace view
    edwprodhh.edi_837p_parser.export_data_dimensions
as
select      *
from        edwprodhh.edi_837p_parser.export_data_dimensions_log
where       upload_date = current_date()
;