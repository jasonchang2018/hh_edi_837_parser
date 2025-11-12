create or replace table
    edwprodhh.edi_837p_parser.claim_service_lines_adjudications
as
with filtered_svd as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is not null
                and svd_index is not null
)
, svd as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SVD_PREFIX'
                            when    flattened.index = 2   then      'SVD_PAYER_ID'
                            when    flattened.index = 3   then      'SVD_PAYER_PAID_AMT'
                            when    flattened.index = 4   then      'SVD_PROCEDURE_CODE'
                            when    flattened.index = 5   then      'SVD_QUANTITY'
                            when    flattened.index = 6   then      'SVD_FACILITY_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^SVD.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SVD_PREFIX',
                        'SVD_PAYER_ID',
                        'SVD_PAYER_PAID_AMT',
                        'SVD_PROCEDURE_CODE',
                        'SVD_QUANTITY',
                        'SVD_FACILITY_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    SVD_INDEX,
                    SVD_PREFIX,
                    SVD_PAYER_ID,
                    SVD_PAYER_PAID_AMT,
                    SVD_PROCEDURE_CODE,
                    SVD_QUANTITY,
                    SVD_FACILITY_CODE
                )
)
, cas as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CAS_PREFIX'
                            when    flattened.index = 2   then      'CAS_ADJ_GROUP_CODE'
                            when    flattened.index = 3   then      'CAS_REASON_CODE_1'
                            when    flattened.index = 4   then      'CAS_ADJ_AMOUNT_1'
                            when    flattened.index = 5   then      'CAS_ADJ_QUANTITY_1'
                            when    flattened.index = 6   then      'CAS_REASON_CODE_2'
                            when    flattened.index = 7   then      'CAS_ADJ_AMOUNT_2'
                            when    flattened.index = 8   then      'CAS_ADJ_QUANTITY_2'
                            when    flattened.index = 9   then      'CAS_REASON_CODE_3'
                            when    flattened.index = 10  then      'CAS_ADJ_AMOUNT_3'
                            when    flattened.index = 11  then      'CAS_ADJ_QUANTITY_3'
                            when    flattened.index = 12  then      'CAS_REASON_CODE_4'
                            when    flattened.index = 13  then      'CAS_ADJ_AMOUNT_4'
                            when    flattened.index = 14  then      'CAS_ADJ_QUANTITY_4'
                            when    flattened.index = 15  then      'CAS_REASON_CODE_5'
                            when    flattened.index = 16  then      'CAS_ADJ_AMOUNT_5'
                            when    flattened.index = 17  then      'CAS_ADJ_QUANTITY_5'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^CAS.*')                          --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'CAS_PREFIX',
                            'CAS_ADJ_GROUP_CODE',
                            'CAS_REASON_CODE_1',
                            'CAS_ADJ_AMOUNT_1',
                            'CAS_ADJ_QUANTITY_1',
                            'CAS_REASON_CODE_2',
                            'CAS_ADJ_AMOUNT_2',
                            'CAS_ADJ_QUANTITY_2',
                            'CAS_REASON_CODE_3',
                            'CAS_ADJ_AMOUNT_3',
                            'CAS_ADJ_QUANTITY_3',
                            'CAS_REASON_CODE_4',
                            'CAS_ADJ_AMOUNT_4',
                            'CAS_ADJ_QUANTITY_4',
                            'CAS_REASON_CODE_5',
                            'CAS_ADJ_AMOUNT_5',
                            'CAS_ADJ_QUANTITY_5'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_TRANSACTION_SET,
                        INDEX,
                        HL_INDEX_CURRENT,
                        HL_INDEX_BILLING_20,
                        HL_INDEX_SUBSCRIBER_22,
                        HL_INDEX_PATIENT_23,
                        CLAIM_INDEX,
                        LX_INDEX,
                        SVD_INDEX,
                        CAS_PREFIX,
                        CAS_ADJ_GROUP_CODE,
                        CAS_REASON_CODE_1,
                        CAS_ADJ_AMOUNT_1,
                        CAS_ADJ_QUANTITY_1,
                        CAS_REASON_CODE_2,
                        CAS_ADJ_AMOUNT_2,
                        CAS_ADJ_QUANTITY_2,
                        CAS_REASON_CODE_3,
                        CAS_ADJ_AMOUNT_3,
                        CAS_ADJ_QUANTITY_3,
                        CAS_REASON_CODE_4,
                        CAS_ADJ_AMOUNT_4,
                        CAS_ADJ_QUANTITY_4,
                        CAS_REASON_CODE_5,
                        CAS_ADJ_AMOUNT_5,
                        CAS_ADJ_QUANTITY_5
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                lx_index,
                svd_index,
                array_agg(
                    object_construct_keep_null(
                        'CAS_ADJ_GROUP_CODE',       cas_adj_group_code::varchar,
                        'CAS_REASON_CODE_1',        cas_reason_code_1::varchar,
                        'CAS_ADJ_AMOUNT_1',         cas_adj_amount_1::varchar,
                        'CAS_ADJ_QUANTITY_1',       cas_adj_quantity_1::varchar,
                        'CAS_REASON_CODE_2',        cas_reason_code_2::varchar,
                        'CAS_ADJ_AMOUNT_2',         cas_adj_amount_2::varchar,
                        'CAS_ADJ_QUANTITY_2',       cas_adj_quantity_2::varchar,
                        'CAS_REASON_CODE_3',        cas_reason_code_3::varchar,
                        'CAS_ADJ_AMOUNT_3',         cas_adj_amount_3::varchar,
                        'CAS_ADJ_QUANTITY_3',       cas_adj_quantity_3::varchar,
                        'CAS_REASON_CODE_4',        cas_reason_code_4::varchar,
                        'CAS_ADJ_AMOUNT_4',         cas_adj_amount_4::varchar,
                        'CAS_ADJ_QUANTITY_4',       cas_adj_quantity_4::varchar,
                        'CAS_REASON_CODE_5',        cas_reason_code_5::varchar,
                        'CAS_ADJ_AMOUNT_5',         cas_adj_amount_5::varchar,
                        'CAS_ADJ_QUANTITY_5',       cas_adj_quantity_5::varchar
                    )
                )   as cas_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, dtp_573 as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_SVD_ADJUDICATION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_SVD_ADJUDICATION'
                            when    flattened.index = 3   then      'DATE_FORMAT_SVD_ADJUDICATION'
                            when    flattened.index = 4   then      'DATE_SVD_ADJUDICATION'
                            end     as value_header,

                    case    when    value_header = 'DATE_SVD_ADJUDICATION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^DTP\\*573.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_SVD_ADJUDICATION',
                        'DATE_QUALIFIER_SVD_ADJUDICATION',
                        'DATE_FORMAT_SVD_ADJUDICATION',
                        'DATE_SVD_ADJUDICATION'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    SVD_INDEX,
                    DTP_PREFIX_SVD_ADJUDICATION,
                    DATE_QUALIFIER_SVD_ADJUDICATION,
                    DATE_FORMAT_SVD_ADJUDICATION,
                    DATE_SVD_ADJUDICATION
                )
)
select      svd.response_id,
            svd.nth_transaction_set,
            svd.index,
            svd.hl_index_current,
            svd.hl_index_billing_20,
            svd.hl_index_subscriber_22,
            svd.hl_index_patient_23,
            svd.claim_index,
            svd.lx_index,
            svd.svd_index,
            svd.svd_prefix,
            svd.svd_payer_id,
            svd.svd_payer_paid_amt,
            svd.svd_procedure_code,
            svd.svd_quantity,
            svd.svd_facility_code,
            dtp_573.dtp_prefix_svd_adjudication,
            dtp_573.date_qualifier_svd_adjudication,
            dtp_573.date_format_svd_adjudication,
            dtp_573.date_svd_adjudication,
            cas.cas_array
from        svd
            left join
                cas
                on  svd.response_id         = cas.response_id
                and svd.nth_transaction_set = cas.nth_transaction_set
                and svd.claim_index         = cas.claim_index
                and svd.lx_index            = cas.lx_index
                and svd.svd_index           = cas.svd_index
            left join
                dtp_573
                on  svd.response_id         = dtp_573.response_id
                and svd.nth_transaction_set = dtp_573.nth_transaction_set
                and svd.claim_index         = dtp_573.claim_index
                and svd.lx_index            = dtp_573.lx_index
                and svd.svd_index           = dtp_573.svd_index
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837p_parser.insert_claim_service_lines_adjudications
    warehouse = analysis_wh
    after edwprodhh.edi_837p_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837p_parser.claim_service_lines_adjudications
(
    RESPONSE_ID,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    CLAIM_INDEX,
    LX_INDEX,
    SVD_INDEX,
    SVD_PREFIX,
    SVD_PAYER_ID,
    SVD_PAYER_PAID_AMT,
    SVD_PROCEDURE_CODE,
    SVD_QUANTITY,
    SVD_FACILITY_CODE,
    DTP_PREFIX_SVD_ADJUDICATION,
    DATE_QUALIFIER_SVD_ADJUDICATION,
    DATE_FORMAT_SVD_ADJUDICATION,
    DATE_SVD_ADJUDICATION,
    CAS_ARRAY
)
with filtered_svd as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_837p_parser.claim_service_lines_adjudications)
                and claim_index is not null --0 Pre-Filter
                and lx_index is not null
                and svd_index is not null
)
, svd as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SVD_PREFIX'
                            when    flattened.index = 2   then      'SVD_PAYER_ID'
                            when    flattened.index = 3   then      'SVD_PAYER_PAID_AMT'
                            when    flattened.index = 4   then      'SVD_PROCEDURE_CODE'
                            when    flattened.index = 5   then      'SVD_QUANTITY'
                            when    flattened.index = 6   then      'SVD_FACILITY_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^SVD.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SVD_PREFIX',
                        'SVD_PAYER_ID',
                        'SVD_PAYER_PAID_AMT',
                        'SVD_PROCEDURE_CODE',
                        'SVD_QUANTITY',
                        'SVD_FACILITY_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    SVD_INDEX,
                    SVD_PREFIX,
                    SVD_PAYER_ID,
                    SVD_PAYER_PAID_AMT,
                    SVD_PROCEDURE_CODE,
                    SVD_QUANTITY,
                    SVD_FACILITY_CODE
                )
)
, cas as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CAS_PREFIX'
                            when    flattened.index = 2   then      'CAS_ADJ_GROUP_CODE'
                            when    flattened.index = 3   then      'CAS_REASON_CODE_1'
                            when    flattened.index = 4   then      'CAS_ADJ_AMOUNT_1'
                            when    flattened.index = 5   then      'CAS_ADJ_QUANTITY_1'
                            when    flattened.index = 6   then      'CAS_REASON_CODE_2'
                            when    flattened.index = 7   then      'CAS_ADJ_AMOUNT_2'
                            when    flattened.index = 8   then      'CAS_ADJ_QUANTITY_2'
                            when    flattened.index = 9   then      'CAS_REASON_CODE_3'
                            when    flattened.index = 10  then      'CAS_ADJ_AMOUNT_3'
                            when    flattened.index = 11  then      'CAS_ADJ_QUANTITY_3'
                            when    flattened.index = 12  then      'CAS_REASON_CODE_4'
                            when    flattened.index = 13  then      'CAS_ADJ_AMOUNT_4'
                            when    flattened.index = 14  then      'CAS_ADJ_QUANTITY_4'
                            when    flattened.index = 15  then      'CAS_REASON_CODE_5'
                            when    flattened.index = 16  then      'CAS_ADJ_AMOUNT_5'
                            when    flattened.index = 17  then      'CAS_ADJ_QUANTITY_5'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^CAS.*')                          --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'CAS_PREFIX',
                            'CAS_ADJ_GROUP_CODE',
                            'CAS_REASON_CODE_1',
                            'CAS_ADJ_AMOUNT_1',
                            'CAS_ADJ_QUANTITY_1',
                            'CAS_REASON_CODE_2',
                            'CAS_ADJ_AMOUNT_2',
                            'CAS_ADJ_QUANTITY_2',
                            'CAS_REASON_CODE_3',
                            'CAS_ADJ_AMOUNT_3',
                            'CAS_ADJ_QUANTITY_3',
                            'CAS_REASON_CODE_4',
                            'CAS_ADJ_AMOUNT_4',
                            'CAS_ADJ_QUANTITY_4',
                            'CAS_REASON_CODE_5',
                            'CAS_ADJ_AMOUNT_5',
                            'CAS_ADJ_QUANTITY_5'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_TRANSACTION_SET,
                        INDEX,
                        HL_INDEX_CURRENT,
                        HL_INDEX_BILLING_20,
                        HL_INDEX_SUBSCRIBER_22,
                        HL_INDEX_PATIENT_23,
                        CLAIM_INDEX,
                        LX_INDEX,
                        SVD_INDEX,
                        CAS_PREFIX,
                        CAS_ADJ_GROUP_CODE,
                        CAS_REASON_CODE_1,
                        CAS_ADJ_AMOUNT_1,
                        CAS_ADJ_QUANTITY_1,
                        CAS_REASON_CODE_2,
                        CAS_ADJ_AMOUNT_2,
                        CAS_ADJ_QUANTITY_2,
                        CAS_REASON_CODE_3,
                        CAS_ADJ_AMOUNT_3,
                        CAS_ADJ_QUANTITY_3,
                        CAS_REASON_CODE_4,
                        CAS_ADJ_AMOUNT_4,
                        CAS_ADJ_QUANTITY_4,
                        CAS_REASON_CODE_5,
                        CAS_ADJ_AMOUNT_5,
                        CAS_ADJ_QUANTITY_5
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                lx_index,
                svd_index,
                array_agg(
                    object_construct_keep_null(
                        'CAS_ADJ_GROUP_CODE',       cas_adj_group_code::varchar,
                        'CAS_REASON_CODE_1',        cas_reason_code_1::varchar,
                        'CAS_ADJ_AMOUNT_1',         cas_adj_amount_1::varchar,
                        'CAS_ADJ_QUANTITY_1',       cas_adj_quantity_1::varchar,
                        'CAS_REASON_CODE_2',        cas_reason_code_2::varchar,
                        'CAS_ADJ_AMOUNT_2',         cas_adj_amount_2::varchar,
                        'CAS_ADJ_QUANTITY_2',       cas_adj_quantity_2::varchar,
                        'CAS_REASON_CODE_3',        cas_reason_code_3::varchar,
                        'CAS_ADJ_AMOUNT_3',         cas_adj_amount_3::varchar,
                        'CAS_ADJ_QUANTITY_3',       cas_adj_quantity_3::varchar,
                        'CAS_REASON_CODE_4',        cas_reason_code_4::varchar,
                        'CAS_ADJ_AMOUNT_4',         cas_adj_amount_4::varchar,
                        'CAS_ADJ_QUANTITY_4',       cas_adj_quantity_4::varchar,
                        'CAS_REASON_CODE_5',        cas_reason_code_5::varchar,
                        'CAS_ADJ_AMOUNT_5',         cas_adj_amount_5::varchar,
                        'CAS_ADJ_QUANTITY_5',       cas_adj_quantity_5::varchar
                    )
                )   as cas_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, dtp_573 as
(
    with long as
    (
        select      filtered_svd.response_id,
                    filtered_svd.nth_transaction_set,
                    filtered_svd.index,
                    filtered_svd.hl_index_current,
                    filtered_svd.hl_index_billing_20,
                    filtered_svd.hl_index_subscriber_22,
                    filtered_svd.hl_index_patient_23,
                    filtered_svd.claim_index,
                    filtered_svd.lx_index,
                    filtered_svd.svd_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_SVD_ADJUDICATION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_SVD_ADJUDICATION'
                            when    flattened.index = 3   then      'DATE_FORMAT_SVD_ADJUDICATION'
                            when    flattened.index = 4   then      'DATE_SVD_ADJUDICATION'
                            end     as value_header,

                    case    when    value_header = 'DATE_SVD_ADJUDICATION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_svd,
                    lateral split_to_table(filtered_svd.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_svd.line_element_837, '^DTP\\*573.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_SVD_ADJUDICATION',
                        'DATE_QUALIFIER_SVD_ADJUDICATION',
                        'DATE_FORMAT_SVD_ADJUDICATION',
                        'DATE_SVD_ADJUDICATION'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    SVD_INDEX,
                    DTP_PREFIX_SVD_ADJUDICATION,
                    DATE_QUALIFIER_SVD_ADJUDICATION,
                    DATE_FORMAT_SVD_ADJUDICATION,
                    DATE_SVD_ADJUDICATION
                )
)
select      svd.response_id,
            svd.nth_transaction_set,
            svd.index,
            svd.hl_index_current,
            svd.hl_index_billing_20,
            svd.hl_index_subscriber_22,
            svd.hl_index_patient_23,
            svd.claim_index,
            svd.lx_index,
            svd.svd_index,
            svd.svd_prefix,
            svd.svd_payer_id,
            svd.svd_payer_paid_amt,
            svd.svd_procedure_code,
            svd.svd_quantity,
            svd.svd_facility_code,
            dtp_573.dtp_prefix_svd_adjudication,
            dtp_573.date_qualifier_svd_adjudication,
            dtp_573.date_format_svd_adjudication,
            dtp_573.date_svd_adjudication,
            cas.cas_array
from        svd
            left join
                cas
                on  svd.response_id         = cas.response_id
                and svd.nth_transaction_set = cas.nth_transaction_set
                and svd.claim_index         = cas.claim_index
                and svd.lx_index            = cas.lx_index
                and svd.svd_index           = cas.svd_index
            left join
                dtp_573
                on  svd.response_id         = dtp_573.response_id
                and svd.nth_transaction_set = dtp_573.nth_transaction_set
                and svd.claim_index         = dtp_573.claim_index
                and svd.lx_index            = dtp_573.lx_index
                and svd.svd_index           = dtp_573.svd_index
                
order by    1,2,3
;