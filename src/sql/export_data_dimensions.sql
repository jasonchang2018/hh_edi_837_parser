create or replace view
    edwprodhh.edi_837_parser.export_data_dimensions
as
with debtor as
(
    select      debtor.debtor_idx,
                ltrim(nullif(trim(dimdebtor.cdn), ''), '0') as cdn,
                ltrim(nullif(trim(dimdebtor.drl), ''), '0') as drl,
                dimdebtor.desk
    from        edwprodhh.pub_jchang.master_debtor as debtor
                inner join
                    edwprodhh.dw.dimdebtor as dimdebtor
                    on debtor.debtor_idx = dimdebtor.debtor_idx
    where       debtor.pl_group = 'IU HEALTH - TPL'
                and dimdebtor.desk in ('IU7', 'I14')
)
, claims as
(
    with file_dates as
    (
        select      distinct
                    response_id,
                    file_date
        from        edwprodhh.edi_837_parser.response_flat
    )
    select      claims.response_id,
                claims.nth_transaction_set,
                claims.index,
                claims.claim_index,
                claims.claim_id,
                ltrim(claims.clm_ref_medical_record_num, '0') as mrn
    from        edwprodhh.edi_837_parser.claims as claims
                inner join
                    file_dates
                    on claims.response_id = file_dates.response_id

    where       claims.clm_ref_medical_record_num is not null
                and mrn in (select drl from debtor) --can return multiple claims; 1:M relationship between Medical Record Num (MRN) to Claim

    qualify     row_number() over ( partition by    claims.clm_ref_medical_record_num,
                                                    claims.claim_id
                                    order by        file_dates.file_date    desc,
                                                    claims.claim_index      desc)           = 1
)
, transaction_set as
(
    select      response.response_id,
                response.index,
                response.nth_transaction_set,
                response.file_date,
                claims.claim_id,
                claims.mrn, --joins to DIMDEBTOR.DRL. Trim leading 0s.
                response.line_element_837 || '~' as line_element_837
    from        edwprodhh.edi_837_parser.response_flat as response
                inner join
                    claims
                    on  response.response_id            = claims.response_id
                    and response.nth_transaction_set    = claims.nth_transaction_set
    order by    1,2
)
, headers as
(
    select      'ISA*00*          *00*          *ZZ*' || rpad('580977458', 15, ' ') || '*ZZ*' || rpad('12345678', 15, ' ') || '*' || to_varchar(current_timestamp(), 'yymmdd*hh24mi') || '*^*00501*000000001*1*P*:~'  as line_element_837,
                -2                                                                                                                              as index
    union all
    select      'GS*HC*580977458*12345678*' || to_varchar(current_timestamp(), 'yyyymmdd*hh24mi') || '*000000001*X*005010X223A2~'            as line_element_837,
                -1                                                                                                                              as index
)
, unioned as
(
    select      line_element_837,
                index
    from        transaction_set
    union all
    select      line_element_837,
                index
    from        headers
)
select      line_element_837
from        unioned
order by    index
;