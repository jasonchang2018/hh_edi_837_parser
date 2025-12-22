create or replace table
    edwprodhh.edi_835_parser.remits
as
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       lx_index is not null
)
, lx as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^LX.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_PREFIX',
                        'LX_ASSIGNED_LINE_NUMBER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, clp as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLP_PREFIX'
                            when    flattened.index = 2   then      'CLP_CLAIM_ID'
                            when    flattened.index = 3   then      'CLP_CLAIM_STATUS_CODE'
                            when    flattened.index = 4   then      'CLP_CLAIM_CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'CLP_CLAIM_PAYMENT_AMOUNT'
                            when    flattened.index = 6   then      'CLP_CLAIM_PATIENT_RESP_AMOUNT'
                            when    flattened.index = 7   then      'CLP_CLAIM_FILING_INDICATOR_CODE'
                            when    flattened.index = 8   then      'CLP_CLAIM_PAYER_CONTROL_NUM'
                            when    flattened.index = 9   then      'CLP_CLAIM_FACILITY_TYPE_CODE'
                            when    flattened.index = 10  then      'CLP_CLAIM_FREQUENCY_TYPE_CODE'
                            when    flattened.index = 11  then      'CLP_CLAIM_PATIENT_STATUS_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^CLP.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLP_PREFIX',
                        'CLP_CLAIM_ID',
                        'CLP_CLAIM_STATUS_CODE',
                        'CLP_CLAIM_CHARGE_AMOUNT',
                        'CLP_CLAIM_PAYMENT_AMOUNT',
                        'CLP_CLAIM_PATIENT_RESP_AMOUNT',
                        'CLP_CLAIM_FILING_INDICATOR_CODE',
                        'CLP_CLAIM_PAYER_CONTROL_NUM',
                        'CLP_CLAIM_FACILITY_TYPE_CODE',
                        'CLP_CLAIM_FREQUENCY_TYPE_CODE',
                        'CLP_CLAIM_PATIENT_STATUS_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    CLP_PREFIX,
                    CLP_CLAIM_ID,
                    CLP_CLAIM_STATUS_CODE,
                    CLP_CLAIM_CHARGE_AMOUNT,
                    CLP_CLAIM_PAYMENT_AMOUNT,
                    CLP_CLAIM_PATIENT_RESP_AMOUNT,
                    CLP_CLAIM_FILING_INDICATOR_CODE,
                    CLP_CLAIM_PAYER_CONTROL_NUM,
                    CLP_CLAIM_FACILITY_TYPE_CODE,
                    CLP_CLAIM_FREQUENCY_TYPE_CODE,
                    CLP_CLAIM_PATIENT_STATUS_CODE
                )
)
, nm1_QC as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_QC_PATIENT_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_QC_PATIENT_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_QC_PATIENT_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_QC_PATIENT_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_QC_PATIENT_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_QC_PATIENT_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_QC_PATIENT_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_QC_PATIENT_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^NM1\\*QC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_QC_PATIENT_NAME_CODE',
                        'NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE',
                        'NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER',
                        'NM1_QC_PATIENT_LAST_NAME_ORG',
                        'NM1_QC_PATIENT_FIRST_NAME',
                        'NM1_QC_PATIENT_MIDDLE_NAME',
                        'NM1_QC_PATIENT_NAME_PREFIX',
                        'NM1_QC_PATIENT_NAME_SUFFIX',
                        'NM1_QC_PATIENT_ID_CODE_QUALIFIER',
                        'NM1_QC_PATIENT_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    NM1_QC_PATIENT_NAME_CODE,
                    NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE,
                    NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER,
                    NM1_QC_PATIENT_LAST_NAME_ORG,
                    NM1_QC_PATIENT_FIRST_NAME,
                    NM1_QC_PATIENT_MIDDLE_NAME,
                    NM1_QC_PATIENT_NAME_PREFIX,
                    NM1_QC_PATIENT_NAME_SUFFIX,
                    NM1_QC_PATIENT_ID_CODE_QUALIFIER,
                    NM1_QC_PATIENT_ID_CODE
                )
)
, nm1_82 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_QC_PROVIDER_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_QC_PROVIDER_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_QC_PROVIDER_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_QC_PROVIDER_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_QC_PROVIDER_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_QC_PROVIDER_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_QC_PROVIDER_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_QC_PROVIDER_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^NM1\\*82.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_QC_PROVIDER_NAME_CODE',
                        'NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE',
                        'NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER',
                        'NM1_QC_PROVIDER_LAST_NAME_ORG',
                        'NM1_QC_PROVIDER_FIRST_NAME',
                        'NM1_QC_PROVIDER_MIDDLE_NAME',
                        'NM1_QC_PROVIDER_NAME_PREFIX',
                        'NM1_QC_PROVIDER_NAME_SUFFIX',
                        'NM1_QC_PROVIDER_ID_CODE_QUALIFIER',
                        'NM1_QC_PROVIDER_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    NM1_QC_PROVIDER_NAME_CODE,
                    NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE,
                    NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER,
                    NM1_QC_PROVIDER_LAST_NAME_ORG,
                    NM1_QC_PROVIDER_FIRST_NAME,
                    NM1_QC_PROVIDER_MIDDLE_NAME,
                    NM1_QC_PROVIDER_NAME_PREFIX,
                    NM1_QC_PROVIDER_NAME_SUFFIX,
                    NM1_QC_PROVIDER_ID_CODE_QUALIFIER,
                    NM1_QC_PROVIDER_ID_CODE
                )
)
, dtm_232 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_232_HEADER'
                            when    flattened.index = 2   then      'DTM_232_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_232_DATE'
                            when    flattened.index = 4   then      'DTM_232_TIME'
                            when    flattened.index = 5   then      'DTM_232_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_232_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_232_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*232.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_232_HEADER',
                        'DTM_232_QUALIFIER',
                        'DTM_232_DATE',
                        'DTM_232_TIME',
                        'DTM_232_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_232_HEADER,
                    DTM_232_QUALIFIER,
                    DTM_232_DATE,
                    DTM_232_TIME,
                    DTM_232_TIMEZONE
                )
)
, dtm_233 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_233_HEADER'
                            when    flattened.index = 2   then      'DTM_233_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_233_DATE'
                            when    flattened.index = 4   then      'DTM_233_TIME'
                            when    flattened.index = 5   then      'DTM_233_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_233_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_233_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*233.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_233_HEADER',
                        'DTM_233_QUALIFIER',
                        'DTM_233_DATE',
                        'DTM_233_TIME',
                        'DTM_233_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_233_HEADER,
                    DTM_233_QUALIFIER,
                    DTM_233_DATE,
                    DTM_233_TIME,
                    DTM_233_TIMEZONE
                )
)
, dtm_050 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_050_HEADER'
                            when    flattened.index = 2   then      'DTM_050_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_050_DATE'
                            when    flattened.index = 4   then      'DTM_050_TIME'
                            when    flattened.index = 5   then      'DTM_050_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_050_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_050_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*050.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_050_HEADER',
                        'DTM_050_QUALIFIER',
                        'DTM_050_DATE',
                        'DTM_050_TIME',
                        'DTM_050_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_050_HEADER,
                    DTM_050_QUALIFIER,
                    DTM_050_DATE,
                    DTM_050_TIME,
                    DTM_050_TIMEZONE
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

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_LX_PREFIX'
                            when    flattened.index = 2   then      'REF_LX_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REF_LX_REF_ID'
                            when    flattened.index = 4   then      'REF_LX_DESCRIPTION'
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
                            'REF_LX_PREFIX',
                            'REF_LX_REF_ID_QUALIFIER',
                            'REF_LX_REF_ID',
                            'REF_LX_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        REF_LX_PREFIX,
                        REF_LX_REF_ID_QUALIFIER,
                        REF_LX_REF_ID,
                        REF_LX_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'lx_ref_code',           ref_lx_ref_id_qualifier::varchar,
                        'lx_ref_value',          ref_lx_ref_id::varchar,
                        'lx_ref_description',    ref_lx_description::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
, amt_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'AMT_LX_PREFIX'
                            when    flattened.index = 2     then      'AMT_LX_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'AMT_LX_MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'AMT_LX_CREDIT_DEBIT_FLAG'
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
                            'AMT_LX_PREFIX',
                            'AMT_LX_QUALIFIER_CODE',
                            'AMT_LX_MONETARY_AMOUNT',
                            'AMT_LX_CREDIT_DEBIT_FLAG'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        AMT_LX_PREFIX,
                        AMT_LX_QUALIFIER_CODE,
                        AMT_LX_MONETARY_AMOUNT,
                        AMT_LX_CREDIT_DEBIT_FLAG
                    )
    )
    select      RESPONSE_ID,
                NTH_FUNCTIONAL_GROUP,
                NTH_TRANSACTION_SET,
                LX_INDEX,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_lx_qualifier_code::varchar,
                        'monetary_amount',      amt_lx_monetary_amount::number(18,2),
                        'credit_debit_flag',    amt_lx_credit_debit_flag::varchar
                    )
                )   as amt_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
select      lx.response_id,
            lx.nth_functional_group,
            lx.nth_transaction_set,
            lx.lx_index,
            lx.lx_prefix,
            lx.lx_assigned_line_number,
            clp.clp_prefix,
            clp.clp_claim_id,
            clp.clp_claim_status_code,
            clp.clp_claim_charge_amount,
            clp.clp_claim_payment_amount,
            clp.clp_claim_patient_resp_amount,
            clp.clp_claim_filing_indicator_code,
            clp.clp_claim_payer_control_num,
            clp.clp_claim_facility_type_code,
            clp.clp_claim_frequency_type_code,
            clp.clp_claim_patient_status_code,
            nm1_qc.nm1_qc_patient_name_code,
            nm1_qc.nm1_qc_patient_entity_identifier_code,
            nm1_qc.nm1_qc_patient_entity_type_qualifier,
            nm1_qc.nm1_qc_patient_last_name_org,
            nm1_qc.nm1_qc_patient_first_name,
            nm1_qc.nm1_qc_patient_middle_name,
            nm1_qc.nm1_qc_patient_name_prefix,
            nm1_qc.nm1_qc_patient_name_suffix,
            nm1_qc.nm1_qc_patient_id_code_qualifier,
            nm1_qc.nm1_qc_patient_id_code,
            nm1_82.nm1_qc_provider_name_code,
            nm1_82.nm1_qc_provider_entity_identifier_code,
            nm1_82.nm1_qc_provider_entity_type_qualifier,
            nm1_82.nm1_qc_provider_last_name_org,
            nm1_82.nm1_qc_provider_first_name,
            nm1_82.nm1_qc_provider_middle_name,
            nm1_82.nm1_qc_provider_name_prefix,
            nm1_82.nm1_qc_provider_name_suffix,
            nm1_82.nm1_qc_provider_id_code_qualifier,
            nm1_82.nm1_qc_provider_id_code,
            dtm_232.dtm_232_header,
            dtm_232.dtm_232_qualifier,
            dtm_232.dtm_232_date,
            dtm_232.dtm_232_time,
            dtm_232.dtm_232_timezone,
            dtm_233.dtm_233_header,
            dtm_233.dtm_233_qualifier,
            dtm_233.dtm_233_date,
            dtm_233.dtm_233_time,
            dtm_233.dtm_233_timezone,
            dtm_050.dtm_050_header,
            dtm_050.dtm_050_qualifier,
            dtm_050.dtm_050_date,
            dtm_050.dtm_050_time,
            dtm_050.dtm_050_timezone,
            ref_array.lx_ref_array,
            amt_array.amt_array
from        lx
            left join
                clp
                on  lx.response_id           = clp.response_id
                and lx.nth_functional_group  = clp.nth_functional_group
                and lx.nth_transaction_set   = clp.nth_transaction_set
                and lx.lx_index              = clp.lx_index
            left join
                nm1_QC
                on  lx.response_id           = nm1_QC.response_id
                and lx.nth_functional_group  = nm1_QC.nth_functional_group
                and lx.nth_transaction_set   = nm1_QC.nth_transaction_set
                and lx.lx_index              = nm1_QC.lx_index
            left join
                nm1_82
                on  lx.response_id           = nm1_82.response_id
                and lx.nth_functional_group  = nm1_82.nth_functional_group
                and lx.nth_transaction_set   = nm1_82.nth_transaction_set
                and lx.lx_index              = nm1_82.lx_index
            left join
                dtm_232
                on  lx.response_id           = dtm_232.response_id
                and lx.nth_functional_group  = dtm_232.nth_functional_group
                and lx.nth_transaction_set   = dtm_232.nth_transaction_set
                and lx.lx_index              = dtm_232.lx_index
            left join
                dtm_233
                on  lx.response_id           = dtm_233.response_id
                and lx.nth_functional_group  = dtm_233.nth_functional_group
                and lx.nth_transaction_set   = dtm_233.nth_transaction_set
                and lx.lx_index              = dtm_233.lx_index
            left join
                dtm_050
                on  lx.response_id           = dtm_050.response_id
                and lx.nth_functional_group  = dtm_050.nth_functional_group
                and lx.nth_transaction_set   = dtm_050.nth_transaction_set
                and lx.lx_index              = dtm_050.lx_index
            left join
                ref_array
                on  lx.response_id           = ref_array.response_id
                and lx.nth_functional_group  = ref_array.nth_functional_group
                and lx.nth_transaction_set   = ref_array.nth_transaction_set
                and lx.lx_index              = ref_array.lx_index
            left join
                amt_array
                on  lx.response_id           = amt_array.response_id
                and lx.nth_functional_group  = amt_array.nth_functional_group
                and lx.nth_transaction_set   = amt_array.nth_transaction_set
                and lx.lx_index              = amt_array.lx_index
;



create or replace task
    edwprodhh.edi_835_parser.insert_remits
    warehouse = analysis_wh
    after edwprodhh.edi_835_parser.insert_response_flat
as
insert into
    edwprodhh.edi_835_parser.remits
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    LX_INDEX,
    LX_PREFIX,
    LX_ASSIGNED_LINE_NUMBER,
    CLP_PREFIX,
    CLP_CLAIM_ID,
    CLP_CLAIM_STATUS_CODE,
    CLP_CLAIM_CHARGE_AMOUNT,
    CLP_CLAIM_PAYMENT_AMOUNT,
    CLP_CLAIM_PATIENT_RESP_AMOUNT,
    CLP_CLAIM_FILING_INDICATOR_CODE,
    CLP_CLAIM_PAYER_CONTROL_NUM,
    CLP_CLAIM_FACILITY_TYPE_CODE,
    CLP_CLAIM_FREQUENCY_TYPE_CODE,
    CLP_CLAIM_PATIENT_STATUS_CODE,
    NM1_QC_PATIENT_NAME_CODE,
    NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE,
    NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER,
    NM1_QC_PATIENT_LAST_NAME_ORG,
    NM1_QC_PATIENT_FIRST_NAME,
    NM1_QC_PATIENT_MIDDLE_NAME,
    NM1_QC_PATIENT_NAME_PREFIX,
    NM1_QC_PATIENT_NAME_SUFFIX,
    NM1_QC_PATIENT_ID_CODE_QUALIFIER,
    NM1_QC_PATIENT_ID_CODE,
    NM1_QC_PROVIDER_NAME_CODE,
    NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE,
    NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER,
    NM1_QC_PROVIDER_LAST_NAME_ORG,
    NM1_QC_PROVIDER_FIRST_NAME,
    NM1_QC_PROVIDER_MIDDLE_NAME,
    NM1_QC_PROVIDER_NAME_PREFIX,
    NM1_QC_PROVIDER_NAME_SUFFIX,
    NM1_QC_PROVIDER_ID_CODE_QUALIFIER,
    NM1_QC_PROVIDER_ID_CODE,
    DTM_232_HEADER,
    DTM_232_QUALIFIER,
    DTM_232_DATE,
    DTM_232_TIME,
    DTM_232_TIMEZONE,
    DTM_233_HEADER,
    DTM_233_QUALIFIER,
    DTM_233_DATE,
    DTM_233_TIME,
    DTM_233_TIMEZONE,
    DTM_050_HEADER,
    DTM_050_QUALIFIER,
    DTM_050_DATE,
    DTM_050_TIME,
    DTM_050_TIMEZONE,
    LX_REF_ARRAY,
    AMT_ARRAY
)
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       lx_index is not null
                and response_id not in (select response_id from edwprodhh.edi_835_parser.remits)
)
, lx as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^LX.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_PREFIX',
                        'LX_ASSIGNED_LINE_NUMBER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, clp as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLP_PREFIX'
                            when    flattened.index = 2   then      'CLP_CLAIM_ID'
                            when    flattened.index = 3   then      'CLP_CLAIM_STATUS_CODE'
                            when    flattened.index = 4   then      'CLP_CLAIM_CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'CLP_CLAIM_PAYMENT_AMOUNT'
                            when    flattened.index = 6   then      'CLP_CLAIM_PATIENT_RESP_AMOUNT'
                            when    flattened.index = 7   then      'CLP_CLAIM_FILING_INDICATOR_CODE'
                            when    flattened.index = 8   then      'CLP_CLAIM_PAYER_CONTROL_NUM'
                            when    flattened.index = 9   then      'CLP_CLAIM_FACILITY_TYPE_CODE'
                            when    flattened.index = 10  then      'CLP_CLAIM_FREQUENCY_TYPE_CODE'
                            when    flattened.index = 11  then      'CLP_CLAIM_PATIENT_STATUS_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_835, '^CLP.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLP_PREFIX',
                        'CLP_CLAIM_ID',
                        'CLP_CLAIM_STATUS_CODE',
                        'CLP_CLAIM_CHARGE_AMOUNT',
                        'CLP_CLAIM_PAYMENT_AMOUNT',
                        'CLP_CLAIM_PATIENT_RESP_AMOUNT',
                        'CLP_CLAIM_FILING_INDICATOR_CODE',
                        'CLP_CLAIM_PAYER_CONTROL_NUM',
                        'CLP_CLAIM_FACILITY_TYPE_CODE',
                        'CLP_CLAIM_FREQUENCY_TYPE_CODE',
                        'CLP_CLAIM_PATIENT_STATUS_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    CLP_PREFIX,
                    CLP_CLAIM_ID,
                    CLP_CLAIM_STATUS_CODE,
                    CLP_CLAIM_CHARGE_AMOUNT,
                    CLP_CLAIM_PAYMENT_AMOUNT,
                    CLP_CLAIM_PATIENT_RESP_AMOUNT,
                    CLP_CLAIM_FILING_INDICATOR_CODE,
                    CLP_CLAIM_PAYER_CONTROL_NUM,
                    CLP_CLAIM_FACILITY_TYPE_CODE,
                    CLP_CLAIM_FREQUENCY_TYPE_CODE,
                    CLP_CLAIM_PATIENT_STATUS_CODE
                )
)
, nm1_QC as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_QC_PATIENT_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_QC_PATIENT_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_QC_PATIENT_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_QC_PATIENT_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_QC_PATIENT_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_QC_PATIENT_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_QC_PATIENT_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_QC_PATIENT_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^NM1\\*QC.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_QC_PATIENT_NAME_CODE',
                        'NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE',
                        'NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER',
                        'NM1_QC_PATIENT_LAST_NAME_ORG',
                        'NM1_QC_PATIENT_FIRST_NAME',
                        'NM1_QC_PATIENT_MIDDLE_NAME',
                        'NM1_QC_PATIENT_NAME_PREFIX',
                        'NM1_QC_PATIENT_NAME_SUFFIX',
                        'NM1_QC_PATIENT_ID_CODE_QUALIFIER',
                        'NM1_QC_PATIENT_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    NM1_QC_PATIENT_NAME_CODE,
                    NM1_QC_PATIENT_ENTITY_IDENTIFIER_CODE,
                    NM1_QC_PATIENT_ENTITY_TYPE_QUALIFIER,
                    NM1_QC_PATIENT_LAST_NAME_ORG,
                    NM1_QC_PATIENT_FIRST_NAME,
                    NM1_QC_PATIENT_MIDDLE_NAME,
                    NM1_QC_PATIENT_NAME_PREFIX,
                    NM1_QC_PATIENT_NAME_SUFFIX,
                    NM1_QC_PATIENT_ID_CODE_QUALIFIER,
                    NM1_QC_PATIENT_ID_CODE
                )
)
, nm1_82 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_QC_PROVIDER_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_QC_PROVIDER_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_QC_PROVIDER_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_QC_PROVIDER_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_QC_PROVIDER_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_QC_PROVIDER_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_QC_PROVIDER_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_QC_PROVIDER_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^NM1\\*82.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_QC_PROVIDER_NAME_CODE',
                        'NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE',
                        'NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER',
                        'NM1_QC_PROVIDER_LAST_NAME_ORG',
                        'NM1_QC_PROVIDER_FIRST_NAME',
                        'NM1_QC_PROVIDER_MIDDLE_NAME',
                        'NM1_QC_PROVIDER_NAME_PREFIX',
                        'NM1_QC_PROVIDER_NAME_SUFFIX',
                        'NM1_QC_PROVIDER_ID_CODE_QUALIFIER',
                        'NM1_QC_PROVIDER_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    NM1_QC_PROVIDER_NAME_CODE,
                    NM1_QC_PROVIDER_ENTITY_IDENTIFIER_CODE,
                    NM1_QC_PROVIDER_ENTITY_TYPE_QUALIFIER,
                    NM1_QC_PROVIDER_LAST_NAME_ORG,
                    NM1_QC_PROVIDER_FIRST_NAME,
                    NM1_QC_PROVIDER_MIDDLE_NAME,
                    NM1_QC_PROVIDER_NAME_PREFIX,
                    NM1_QC_PROVIDER_NAME_SUFFIX,
                    NM1_QC_PROVIDER_ID_CODE_QUALIFIER,
                    NM1_QC_PROVIDER_ID_CODE
                )
)
, dtm_232 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_232_HEADER'
                            when    flattened.index = 2   then      'DTM_232_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_232_DATE'
                            when    flattened.index = 4   then      'DTM_232_TIME'
                            when    flattened.index = 5   then      'DTM_232_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_232_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_232_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*232.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_232_HEADER',
                        'DTM_232_QUALIFIER',
                        'DTM_232_DATE',
                        'DTM_232_TIME',
                        'DTM_232_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_232_HEADER,
                    DTM_232_QUALIFIER,
                    DTM_232_DATE,
                    DTM_232_TIME,
                    DTM_232_TIMEZONE
                )
)
, dtm_233 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_233_HEADER'
                            when    flattened.index = 2   then      'DTM_233_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_233_DATE'
                            when    flattened.index = 4   then      'DTM_233_TIME'
                            when    flattened.index = 5   then      'DTM_233_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_233_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_233_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*233.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_233_HEADER',
                        'DTM_233_QUALIFIER',
                        'DTM_233_DATE',
                        'DTM_233_TIME',
                        'DTM_233_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_233_HEADER,
                    DTM_233_QUALIFIER,
                    DTM_233_DATE,
                    DTM_233_TIME,
                    DTM_233_TIMEZONE
                )
)
, dtm_050 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_050_HEADER'
                            when    flattened.index = 2   then      'DTM_050_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_050_DATE'
                            when    flattened.index = 4   then      'DTM_050_TIME'
                            when    flattened.index = 5   then      'DTM_050_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_050_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_050_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*050.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_050_HEADER',
                        'DTM_050_QUALIFIER',
                        'DTM_050_DATE',
                        'DTM_050_TIME',
                        'DTM_050_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    LX_INDEX,
                    DTM_050_HEADER,
                    DTM_050_QUALIFIER,
                    DTM_050_DATE,
                    DTM_050_TIME,
                    DTM_050_TIMEZONE
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

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_LX_PREFIX'
                            when    flattened.index = 2   then      'REF_LX_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REF_LX_REF_ID'
                            when    flattened.index = 4   then      'REF_LX_DESCRIPTION'
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
                            'REF_LX_PREFIX',
                            'REF_LX_REF_ID_QUALIFIER',
                            'REF_LX_REF_ID',
                            'REF_LX_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        REF_LX_PREFIX,
                        REF_LX_REF_ID_QUALIFIER,
                        REF_LX_REF_ID,
                        REF_LX_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'lx_ref_code',           ref_lx_ref_id_qualifier::varchar,
                        'lx_ref_value',          ref_lx_ref_id::varchar,
                        'lx_ref_description',    ref_lx_description::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
, amt_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then      'AMT_LX_PREFIX'
                            when    flattened.index = 2     then      'AMT_LX_QUALIFIER_CODE'
                            when    flattened.index = 3     then      'AMT_LX_MONETARY_AMOUNT'
                            when    flattened.index = 4     then      'AMT_LX_CREDIT_DEBIT_FLAG'
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
                            'AMT_LX_PREFIX',
                            'AMT_LX_QUALIFIER_CODE',
                            'AMT_LX_MONETARY_AMOUNT',
                            'AMT_LX_CREDIT_DEBIT_FLAG'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        LX_INDEX,
                        AMT_LX_PREFIX,
                        AMT_LX_QUALIFIER_CODE,
                        AMT_LX_MONETARY_AMOUNT,
                        AMT_LX_CREDIT_DEBIT_FLAG
                    )
    )
    select      RESPONSE_ID,
                NTH_FUNCTIONAL_GROUP,
                NTH_TRANSACTION_SET,
                LX_INDEX,
                array_agg(
                    object_construct_keep_null(
                        'amt_qualifier_code',   amt_lx_qualifier_code::varchar,
                        'monetary_amount',      amt_lx_monetary_amount::number(18,2),
                        'credit_debit_flag',    amt_lx_credit_debit_flag::varchar
                    )
                )   as amt_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
select      lx.response_id,
            lx.nth_functional_group,
            lx.nth_transaction_set,
            lx.lx_index,
            lx.lx_prefix,
            lx.lx_assigned_line_number,
            clp.clp_prefix,
            clp.clp_claim_id,
            clp.clp_claim_status_code,
            clp.clp_claim_charge_amount,
            clp.clp_claim_payment_amount,
            clp.clp_claim_patient_resp_amount,
            clp.clp_claim_filing_indicator_code,
            clp.clp_claim_payer_control_num,
            clp.clp_claim_facility_type_code,
            clp.clp_claim_frequency_type_code,
            clp.clp_claim_patient_status_code,
            nm1_qc.nm1_qc_patient_name_code,
            nm1_qc.nm1_qc_patient_entity_identifier_code,
            nm1_qc.nm1_qc_patient_entity_type_qualifier,
            nm1_qc.nm1_qc_patient_last_name_org,
            nm1_qc.nm1_qc_patient_first_name,
            nm1_qc.nm1_qc_patient_middle_name,
            nm1_qc.nm1_qc_patient_name_prefix,
            nm1_qc.nm1_qc_patient_name_suffix,
            nm1_qc.nm1_qc_patient_id_code_qualifier,
            nm1_qc.nm1_qc_patient_id_code,
            nm1_82.nm1_qc_provider_name_code,
            nm1_82.nm1_qc_provider_entity_identifier_code,
            nm1_82.nm1_qc_provider_entity_type_qualifier,
            nm1_82.nm1_qc_provider_last_name_org,
            nm1_82.nm1_qc_provider_first_name,
            nm1_82.nm1_qc_provider_middle_name,
            nm1_82.nm1_qc_provider_name_prefix,
            nm1_82.nm1_qc_provider_name_suffix,
            nm1_82.nm1_qc_provider_id_code_qualifier,
            nm1_82.nm1_qc_provider_id_code,
            dtm_232.dtm_232_header,
            dtm_232.dtm_232_qualifier,
            dtm_232.dtm_232_date,
            dtm_232.dtm_232_time,
            dtm_232.dtm_232_timezone,
            dtm_233.dtm_233_header,
            dtm_233.dtm_233_qualifier,
            dtm_233.dtm_233_date,
            dtm_233.dtm_233_time,
            dtm_233.dtm_233_timezone,
            dtm_050.dtm_050_header,
            dtm_050.dtm_050_qualifier,
            dtm_050.dtm_050_date,
            dtm_050.dtm_050_time,
            dtm_050.dtm_050_timezone,
            ref_array.lx_ref_array,
            amt_array.amt_array
from        lx
            left join
                clp
                on  lx.response_id           = clp.response_id
                and lx.nth_functional_group  = clp.nth_functional_group
                and lx.nth_transaction_set   = clp.nth_transaction_set
                and lx.lx_index              = clp.lx_index
            left join
                nm1_QC
                on  lx.response_id           = nm1_QC.response_id
                and lx.nth_functional_group  = nm1_QC.nth_functional_group
                and lx.nth_transaction_set   = nm1_QC.nth_transaction_set
                and lx.lx_index              = nm1_QC.lx_index
            left join
                nm1_82
                on  lx.response_id           = nm1_82.response_id
                and lx.nth_functional_group  = nm1_82.nth_functional_group
                and lx.nth_transaction_set   = nm1_82.nth_transaction_set
                and lx.lx_index              = nm1_82.lx_index
            left join
                dtm_232
                on  lx.response_id           = dtm_232.response_id
                and lx.nth_functional_group  = dtm_232.nth_functional_group
                and lx.nth_transaction_set   = dtm_232.nth_transaction_set
                and lx.lx_index              = dtm_232.lx_index
            left join
                dtm_233
                on  lx.response_id           = dtm_233.response_id
                and lx.nth_functional_group  = dtm_233.nth_functional_group
                and lx.nth_transaction_set   = dtm_233.nth_transaction_set
                and lx.lx_index              = dtm_233.lx_index
            left join
                dtm_050
                on  lx.response_id           = dtm_050.response_id
                and lx.nth_functional_group  = dtm_050.nth_functional_group
                and lx.nth_transaction_set   = dtm_050.nth_transaction_set
                and lx.lx_index              = dtm_050.lx_index
            left join
                ref_array
                on  lx.response_id           = ref_array.response_id
                and lx.nth_functional_group  = ref_array.nth_functional_group
                and lx.nth_transaction_set   = ref_array.nth_transaction_set
                and lx.lx_index              = ref_array.lx_index
            left join
                amt_array
                on  lx.response_id           = amt_array.response_id
                and lx.nth_functional_group  = amt_array.nth_functional_group
                and lx.nth_transaction_set   = amt_array.nth_transaction_set
                and lx.lx_index              = amt_array.lx_index
;