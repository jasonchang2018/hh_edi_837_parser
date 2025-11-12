create or replace view
    edwprodhh.edi_837p_parser.export_data_dimensions
as
with debtor as
(
    select      debtor.debtor_idx,
                case    when    debtor.client_idx = 'HH-2175NLIHB'
                        then    'HB'
                        when    debtor.client_idx = 'HH-2175NLIPB'
                        then    'PB'
                        end     as hbpb,
                ltrim(nullif(trim(dimdebtor.cdn), ''), '0') as cdn,
                ltrim(nullif(trim(dimdebtor.drl), ''), '0') as drl,
                dimdebtor.desk
    from        edwprodhh.pub_jchang.master_debtor as debtor
                inner join
                    edwprodhh.dw.dimdebtor as dimdebtor
                    on debtor.debtor_idx = dimdebtor.debtor_idx
                inner join
                    edwprodhh.edi_837p_parser.export_data_dimensions_accounts as accounts
                    on debtor.debtor_idx = accounts.debtor_idx
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
        from        edwprodhh.edi_837p_parser.response_flat
    )
    , debtor_unique as
    (
        select      *
        from        debtor
        qualify     row_number() over (partition by drl order by debtor_idx desc) = 1
    )
    select      claims.response_id,
                claims.nth_transaction_set,
                claims.index,
                claims.claim_index,
                claims.claim_id,
                ltrim(claims.clm_ref_medical_record_num, '0') as mrn,
                debtor_unique.debtor_idx,
                debtor_unique.hbpb
    from        edwprodhh.edi_837p_parser.claims as claims
                inner join
                    file_dates
                    on claims.response_id = file_dates.response_id
                inner join
                    debtor_unique
                    on ltrim(claims.clm_ref_medical_record_num, '0') = debtor_unique.drl --can return multiple claims; 1:M relationship between Medical Record Num (MRN) to Claim

    where       claims.clm_ref_medical_record_num is not null

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
                response.line_element_837 || '~' as line_element_837,
                claims.debtor_idx,
                claims.hbpb
    from        edwprodhh.edi_837p_parser.response_flat as response
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
                index,
                hbpb
    from        transaction_set
    union all
    select      line_element_837,
                index,
                'BOTH' as hbpb
    from        headers
)
select      *
from        unioned
order by    index
;