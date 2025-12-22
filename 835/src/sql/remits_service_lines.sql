create or replace table
    edwprodhh.edi_835_parser.remits_service_lines
as
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       lx_index is not null
                and svc_index is not null
)
, svc as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SVC_PREFIX'
                            when    flattened.index = 2   then      'SVC_PROCEDURE_ID'
                            when    flattened.index = 3   then      'SVC_CHARGE_AMOUNT'
                            when    flattened.index = 4   then      'SVC_PAYMENT_AMOUNT'
                            when    flattened.index = 5   then      'SVC_REVENUE_CODE'
                            when    flattened.index = 6   then      'SVC_UNITS_PAID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^SVC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SVC_PREFIX',
                        'SVC_PROCEDURE_ID',
                        'SVC_CHARGE_AMOUNT',
                        'SVC_PAYMENT_AMOUNT',
                        'SVC_REVENUE_CODE',
                        'SVC_UNITS_PAID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    SVC_PREFIX,
                    SVC_PROCEDURE_ID,
                    SVC_CHARGE_AMOUNT,
                    SVC_PAYMENT_AMOUNT,
                    SVC_REVENUE_CODE,
                    SVC_UNITS_PAID
                )
)
, dtm_472 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_472_HEADER'
                            when    flattened.index = 2   then      'DTM_472_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_472_DATE'
                            when    flattened.index = 4   then      'DTM_472_TIME'
                            when    flattened.index = 5   then      'DTM_472_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_472_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_472_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*472.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_472_HEADER',
                        'DTM_472_QUALIFIER',
                        'DTM_472_DATE',
                        'DTM_472_TIME',
                        'DTM_472_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    DTM_472_HEADER,
                    DTM_472_QUALIFIER,
                    DTM_472_DATE,
                    DTM_472_TIME,
                    DTM_472_TIMEZONE
                )
)
, cas as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

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

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^CAS.*')                         --1 Filter
    )
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
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
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
, lq as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LQ_PREFIX'
                            when    flattened.index = 2   then      'LQ_CODE_LIST_QUALIFIER'
                            when    flattened.index = 3   then      'LQ_INDUSTRY_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^LQ.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LQ_PREFIX',
                        'LQ_CODE_LIST_QUALIFIER',
                        'LQ_INDUSTRY_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    LQ_PREFIX,
                    LQ_CODE_LIST_QUALIFIER,
                    LQ_INDUSTRY_CODE
                )
)
, ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_SVC_PREFIX'
                            when    flattened.index = 2   then      'REF_SVC_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REF_SVC_REF_ID'
                            when    flattened.index = 4   then      'REF_SVC_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_SVC_PREFIX',
                            'REF_SVC_REF_ID_QUALIFIER',
                            'REF_SVC_REF_ID',
                            'REF_SVC_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        SVC_INDEX,
                        REF_SVC_PREFIX,
                        REF_SVC_REF_ID_QUALIFIER,
                        REF_SVC_REF_ID,
                        REF_SVC_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                lx_index,
                svc_index,
                array_agg(
                    object_construct_keep_null(
                        'svc_ref_code',           ref_svc_ref_id_qualifier::varchar,
                        'svc_ref_value',          ref_svc_ref_id::varchar,
                        'svc_ref_description',    ref_svc_description::varchar
                    )
                )   as svc_ref_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, amt_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'AMT_SVC_PREFIX'
                            when    flattened.index = 2     then      'AMT_SVC_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'AMT_SVC_MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'AMT_SVC_CREDIT_DEBIT_FLAG'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^AMT.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'AMT_SVC_PREFIX',
                            'AMT_SVC_QUALIFIER_CODE',
                            'AMT_SVC_MONETARY_AMOUNT',
                            'AMT_SVC_CREDIT_DEBIT_FLAG'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        SVC_INDEX,
                        AMT_SVC_PREFIX,
                        AMT_SVC_QUALIFIER_CODE,
                        AMT_SVC_MONETARY_AMOUNT,
                        AMT_SVC_CREDIT_DEBIT_FLAG
                    )
    )
    select      RESPONSE_ID,
                NTH_FUNCTIONAL_GROUP,
                NTH_TRANSACTION_SET,
                LX_INDEX,
                SVC_INDEX,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_svc_qualifier_code::varchar,
                        'monetary_amount',      amt_svc_monetary_amount::number(18,2),
                        'credit_debit_flag',    amt_svc_credit_debit_flag::varchar
                    )
                )   as amt_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
select      svc.response_id,
            svc.nth_functional_group,
            svc.nth_transaction_set,
            svc.lx_index,
            svc.svc_index,
            svc.svc_prefix,
            svc.svc_procedure_id,
            svc.svc_charge_amount,
            svc.svc_payment_amount,
            svc.svc_revenue_code,
            svc.svc_units_paid,
            dtm_472.dtm_472_header,
            dtm_472.dtm_472_qualifier,
            dtm_472.dtm_472_date,
            dtm_472.dtm_472_time,
            dtm_472.dtm_472_timezone,
            cas.cas_prefix,
            cas.cas_adj_group_code,
            cas.cas_reason_code_1,
            cas.cas_adj_amount_1,
            cas.cas_adj_quantity_1,
            cas.cas_reason_code_2,
            cas.cas_adj_amount_2,
            cas.cas_adj_quantity_2,
            cas.cas_reason_code_3,
            cas.cas_adj_amount_3,
            cas.cas_adj_quantity_3,
            cas.cas_reason_code_4,
            cas.cas_adj_amount_4,
            cas.cas_adj_quantity_4,
            cas.cas_reason_code_5,
            cas.cas_adj_amount_5,
            cas.cas_adj_quantity_5,
            lq.lq_prefix,
            lq.lq_code_list_qualifier,
            lq.lq_industry_code,
            ref_array.svc_ref_array,
            amt_array.amt_array
from        svc
            left join
                dtm_472
                on  svc.response_id           = dtm_472.response_id
                and svc.nth_functional_group  = dtm_472.nth_functional_group
                and svc.nth_transaction_set   = dtm_472.nth_transaction_set
                and svc.lx_index              = dtm_472.lx_index
                and svc.svc_index             = dtm_472.svc_index
            left join
                cas
                on  svc.response_id           = cas.response_id
                and svc.nth_functional_group  = cas.nth_functional_group
                and svc.nth_transaction_set   = cas.nth_transaction_set
                and svc.lx_index              = cas.lx_index
                and svc.svc_index             = cas.svc_index
            left join
                lq
                on  svc.response_id           = lq.response_id
                and svc.nth_functional_group  = lq.nth_functional_group
                and svc.nth_transaction_set   = lq.nth_transaction_set
                and svc.lx_index              = lq.lx_index
                and svc.svc_index             = lq.svc_index
            left join
                ref_array
                on  svc.response_id           = ref_array.response_id
                and svc.nth_functional_group  = ref_array.nth_functional_group
                and svc.nth_transaction_set   = ref_array.nth_transaction_set
                and svc.lx_index              = ref_array.lx_index
                and svc.svc_index             = ref_array.svc_index
            left join
                amt_array
                on  svc.response_id           = amt_array.response_id
                and svc.nth_functional_group  = amt_array.nth_functional_group
                and svc.nth_transaction_set   = amt_array.nth_transaction_set
                and svc.lx_index              = amt_array.lx_index
                and svc.svc_index             = amt_array.svc_index
;



create or replace task
    edwprodhh.edi_835_parser.insert_remits_service_lines
    warehouse = analysis_wh
    after edwprodhh.edi_835_parser.insert_response_flat
as
insert into
    edwprodhh.edi_835_parser.remits_service_lines
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    LX_INDEX,
    SVC_INDEX,
    SVC_PREFIX,
    SVC_PROCEDURE_ID,
    SVC_CHARGE_AMOUNT,
    SVC_PAYMENT_AMOUNT,
    SVC_REVENUE_CODE,
    SVC_UNITS_PAID,
    DTM_472_HEADER,
    DTM_472_QUALIFIER,
    DTM_472_DATE,
    DTM_472_TIME,
    DTM_472_TIMEZONE,
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
    CAS_ADJ_QUANTITY_5,
    LQ_PREFIX,
    LQ_CODE_LIST_QUALIFIER,
    LQ_INDUSTRY_CODE,
    SVC_REF_ARRAY,
    AMT_ARRAY
)
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       lx_index is not null
                and svc_index is not null
                and response_id not in (select response_id from edwprodhh.edi_835_parser.remits_service_lines)
)
, svc as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SVC_PREFIX'
                            when    flattened.index = 2   then      'SVC_PROCEDURE_ID'
                            when    flattened.index = 3   then      'SVC_CHARGE_AMOUNT'
                            when    flattened.index = 4   then      'SVC_PAYMENT_AMOUNT'
                            when    flattened.index = 5   then      'SVC_REVENUE_CODE'
                            when    flattened.index = 6   then      'SVC_UNITS_PAID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^SVC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SVC_PREFIX',
                        'SVC_PROCEDURE_ID',
                        'SVC_CHARGE_AMOUNT',
                        'SVC_PAYMENT_AMOUNT',
                        'SVC_REVENUE_CODE',
                        'SVC_UNITS_PAID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    SVC_PREFIX,
                    SVC_PROCEDURE_ID,
                    SVC_CHARGE_AMOUNT,
                    SVC_PAYMENT_AMOUNT,
                    SVC_REVENUE_CODE,
                    SVC_UNITS_PAID
                )
)
, dtm_472 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_472_HEADER'
                            when    flattened.index = 2   then      'DTM_472_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_472_DATE'
                            when    flattened.index = 4   then      'DTM_472_TIME'
                            when    flattened.index = 5   then      'DTM_472_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_472_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_472_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*472.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_472_HEADER',
                        'DTM_472_QUALIFIER',
                        'DTM_472_DATE',
                        'DTM_472_TIME',
                        'DTM_472_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    DTM_472_HEADER,
                    DTM_472_QUALIFIER,
                    DTM_472_DATE,
                    DTM_472_TIME,
                    DTM_472_TIMEZONE
                )
)
, cas as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

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

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^CAS.*')                         --1 Filter
    )
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
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
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
, lq as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LQ_PREFIX'
                            when    flattened.index = 2   then      'LQ_CODE_LIST_QUALIFIER'
                            when    flattened.index = 3   then      'LQ_INDUSTRY_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^LQ.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LQ_PREFIX',
                        'LQ_CODE_LIST_QUALIFIER',
                        'LQ_INDUSTRY_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    SVC_INDEX,
                    LQ_PREFIX,
                    LQ_CODE_LIST_QUALIFIER,
                    LQ_INDUSTRY_CODE
                )
)
, ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_SVC_PREFIX'
                            when    flattened.index = 2   then      'REF_SVC_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REF_SVC_REF_ID'
                            when    flattened.index = 4   then      'REF_SVC_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_SVC_PREFIX',
                            'REF_SVC_REF_ID_QUALIFIER',
                            'REF_SVC_REF_ID',
                            'REF_SVC_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        SVC_INDEX,
                        REF_SVC_PREFIX,
                        REF_SVC_REF_ID_QUALIFIER,
                        REF_SVC_REF_ID,
                        REF_SVC_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                lx_index,
                svc_index,
                array_agg(
                    object_construct_keep_null(
                        'svc_ref_code',           ref_svc_ref_id_qualifier::varchar,
                        'svc_ref_value',          ref_svc_ref_id::varchar,
                        'svc_ref_description',    ref_svc_description::varchar
                    )
                )   as svc_ref_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
, amt_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,
                    filtered.svc_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'AMT_SVC_PREFIX'
                            when    flattened.index = 2     then      'AMT_SVC_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'AMT_SVC_MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'AMT_SVC_CREDIT_DEBIT_FLAG'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^AMT.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'AMT_SVC_PREFIX',
                            'AMT_SVC_QUALIFIER_CODE',
                            'AMT_SVC_MONETARY_AMOUNT',
                            'AMT_SVC_CREDIT_DEBIT_FLAG'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        SVC_INDEX,
                        AMT_SVC_PREFIX,
                        AMT_SVC_QUALIFIER_CODE,
                        AMT_SVC_MONETARY_AMOUNT,
                        AMT_SVC_CREDIT_DEBIT_FLAG
                    )
    )
    select      RESPONSE_ID,
                NTH_FUNCTIONAL_GROUP,
                NTH_TRANSACTION_SET,
                LX_INDEX,
                SVC_INDEX,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_svc_qualifier_code::varchar,
                        'monetary_amount',      amt_svc_monetary_amount::number(18,2),
                        'credit_debit_flag',    amt_svc_credit_debit_flag::varchar
                    )
                )   as amt_array
    from        pivoted
    group by    1,2,3,4,5
    order by    1,2,3,4,5
)
select      svc.response_id,
            svc.nth_functional_group,
            svc.nth_transaction_set,
            svc.lx_index,
            svc.svc_index,
            svc.svc_prefix,
            svc.svc_procedure_id,
            svc.svc_charge_amount,
            svc.svc_payment_amount,
            svc.svc_revenue_code,
            svc.svc_units_paid,
            dtm_472.dtm_472_header,
            dtm_472.dtm_472_qualifier,
            dtm_472.dtm_472_date,
            dtm_472.dtm_472_time,
            dtm_472.dtm_472_timezone,
            cas.cas_prefix,
            cas.cas_adj_group_code,
            cas.cas_reason_code_1,
            cas.cas_adj_amount_1,
            cas.cas_adj_quantity_1,
            cas.cas_reason_code_2,
            cas.cas_adj_amount_2,
            cas.cas_adj_quantity_2,
            cas.cas_reason_code_3,
            cas.cas_adj_amount_3,
            cas.cas_adj_quantity_3,
            cas.cas_reason_code_4,
            cas.cas_adj_amount_4,
            cas.cas_adj_quantity_4,
            cas.cas_reason_code_5,
            cas.cas_adj_amount_5,
            cas.cas_adj_quantity_5,
            lq.lq_prefix,
            lq.lq_code_list_qualifier,
            lq.lq_industry_code,
            ref_array.svc_ref_array,
            amt_array.amt_array
from        svc
            left join
                dtm_472
                on  svc.response_id           = dtm_472.response_id
                and svc.nth_functional_group  = dtm_472.nth_functional_group
                and svc.nth_transaction_set   = dtm_472.nth_transaction_set
                and svc.lx_index              = dtm_472.lx_index
                and svc.svc_index             = dtm_472.svc_index
            left join
                cas
                on  svc.response_id           = cas.response_id
                and svc.nth_functional_group  = cas.nth_functional_group
                and svc.nth_transaction_set   = cas.nth_transaction_set
                and svc.lx_index              = cas.lx_index
                and svc.svc_index             = cas.svc_index
            left join
                lq
                on  svc.response_id           = lq.response_id
                and svc.nth_functional_group  = lq.nth_functional_group
                and svc.nth_transaction_set   = lq.nth_transaction_set
                and svc.lx_index              = lq.lx_index
                and svc.svc_index             = lq.svc_index
            left join
                ref_array
                on  svc.response_id           = ref_array.response_id
                and svc.nth_functional_group  = ref_array.nth_functional_group
                and svc.nth_transaction_set   = ref_array.nth_transaction_set
                and svc.lx_index              = ref_array.lx_index
                and svc.svc_index             = ref_array.svc_index
            left join
                amt_array
                on  svc.response_id           = amt_array.response_id
                and svc.nth_functional_group  = amt_array.nth_functional_group
                and svc.nth_transaction_set   = amt_array.nth_transaction_set
                and svc.lx_index              = amt_array.lx_index
                and svc.svc_index             = amt_array.svc_index
;