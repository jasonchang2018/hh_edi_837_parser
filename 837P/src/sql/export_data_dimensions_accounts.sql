create table
    edwprodhh.edi_837p_parser.export_data_dimensions_accounts_log
(
    DEBTOR_IDX      VARCHAR(50),
    DRL             VARCHAR(50),
    CDN             VARCHAR(30),
    PL_GROUP        VARCHAR(16777216),
    UPLOAD_DATE     DATE
)
;


create or replace task
    edwprodhh.edi_837p_parser.insert_export_data_dimensions_accounts_log
    warehouse = analysis_wh
    schedule = 'USING CRON 0 4 * * * America/Chicago'
as
insert into
    edwprodhh.edi_837p_parser.export_data_dimensions_accounts_log
with filtered as
(
    select      debtor.debtor_idx,
                ltrim(nullif(trim(dimdebtor.drl), ''), '0')     as drl_,     --MRN
                ltrim(nullif(trim(dimdebtor.cdn), ''), '0')     as cdn_,     --CLAIM_ID
                debtor.pl_group,

    from        edwprodhh.pub_jchang.master_debtor as debtor
                inner join
                    edwprodhh.dw.dimdebtor as dimdebtor
                    on debtor.debtor_idx = dimdebtor.debtor_idx

    where       case    when    debtor.pl_group = 'IU HEALTH - TPL'
                        then    case    when    debtor.client_idx = 'HH-2175NLIPB'
                                        and     dimdebtor.desk in ('I14')
                                        and     debtor.status = 'PFS'
                                        then    TRUE
                                        else    FALSE
                                        end
                        else    FALSE
                        end
                and cdn_ not in (select claim_id from edwprodhh.edi_837p_parser.export_data_dimensions_log where claim_id is not null)                      --if sent to vendor, never selected again
                and cdn_ not in (select cdn from edwprodhh.edi_837p_parser.export_data_dimensions_accounts_log where upload_date >= current_date() - 14)    --if selected but not sent to vendor, try again after cooldown period
    limit       100
)
select      debtor_idx,
            drl_            as drl,
            cdn_            as cdn,
            pl_group        as pl_group,
            current_date()  as upload_date
from        filtered
;


create or replace view
    edwprodhh.edi_837p_parser.export_data_dimensions_accounts
as
select      *
from        edwprodhh.edi_837p_parser.export_data_dimensions_accounts_log
where       upload_date = current_date()
;