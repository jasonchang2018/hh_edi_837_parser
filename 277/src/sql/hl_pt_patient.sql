create or replace table
    edwprodhh.edi_277_parser.hl_pt_patient
as
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = 'PT'
)
, hl as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HL_19_PREFIX'
                            when    flattened.index = 2   then      'HL_19_ID'
                            when    flattened.index = 3   then      'HL_19_PARENT_ID'
                            when    flattened.index = 4   then      'HL_19_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_19_CHILD_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^HL.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'HL_19_PREFIX',
                        'HL_19_ID',
                        'HL_19_PARENT_ID',
                        'HL_19_LEVEL_CODE',
                        'HL_19_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_19_PREFIX,
                    HL_19_ID,
                    HL_19_PARENT_ID,
                    HL_19_LEVEL_CODE,
                    HL_19_CHILD_CODE
                )
)
, nm1_QC as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*QC.*')                         --1 Filter
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
                    HL_INDEX,
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
, trn as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRN_PREFIX'
                            when    flattened.index = 2   then      'TRN_TRACE_TYPE'
                            when    flattened.index = 3   then      'TRN_TRACE_ID'
                            when    flattened.index = 4   then      'TRN_ORIGINATING_COMPANY_IDENTIFIER'
                            when    flattened.index = 5   then      'TRN_REF_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^TRN.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRN_PREFIX',
                        'TRN_TRACE_TYPE',
                        'TRN_TRACE_ID',
                        'TRN_ORIGINATING_COMPANY_IDENTIFIER',
                        'TRN_REF_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    TRN_PREFIX,
                    TRN_TRACE_TYPE,
                    TRN_TRACE_ID,
                    TRN_ORIGINATING_COMPANY_IDENTIFIER,
                    TRN_REF_ID
                )
)
, stc as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'STC_PREFIX'
                            when    flattened.index = 2   then      'STC_STATUS_CATEGORY_CODE'
                            when    flattened.index = 3   then      'STC_DATE'
                            when    flattened.index = 4   then      'STC_ACTION_CODE'
                            when    flattened.index = 5   then      'STC_MONETARY_AMOUNT'
                            when    flattened.index = 6   then      'STC_PAYMENT_METHOD'
                            when    flattened.index = 7   then      'STC_DATE2'
                            when    flattened.index = 8   then      'STC_CHECK_NUMBER'
                            when    flattened.index = 9   then      'STC_REMIT_DATE'
                            end     as value_header,

                    case    when    value_header = 'STC_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^STC.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'STC_PREFIX',
                        'STC_STATUS_CATEGORY_CODE',
                        'STC_DATE',
                        'STC_ACTION_CODE',
                        'STC_MONETARY_AMOUNT',
                        'STC_PAYMENT_METHOD',
                        'STC_DATE2',
                        'STC_CHECK_NUMBER',
                        'STC_REMIT_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    STC_PREFIX,
                    STC_STATUS_CATEGORY_CODE,
                    STC_DATE,
                    STC_ACTION_CODE,
                    STC_MONETARY_AMOUNT,
                    STC_PAYMENT_METHOD,
                    STC_DATE2,
                    STC_CHECK_NUMBER,
                    STC_REMIT_DATE
                )
)
, dtp_472 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_472_PREFIX'
                            when    flattened.index = 2   then      'DTP_472_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_472_FORMAT'
                            when    flattened.index = 4   then      'DTP_472_DATE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*472.*')                          --1 Filter
    )
    select      *,

                case    when    regexp_like(dtp_472_date, '^\\d{8}$')
                        then    to_date(dtp_472_date, 'YYYYMMDD')::varchar
                        when    regexp_like(dtp_472_date, '^\\d{8}\\-\\d{8}$')
                        then    to_date(regexp_substr(dtp_472_date, '^(\\d{8})'), 'YYYYMMDD')::varchar
                        end     as dtp_472_date_start,

                case    when    regexp_like(dtp_472_date, '^\\d{8}$')
                        then    to_date(dtp_472_date, 'YYYYMMDD')::varchar
                        when    regexp_like(dtp_472_date, '^\\d{8}\\-\\d{8}$')
                        then    to_date(regexp_substr(dtp_472_date, '(\\d{8})$'), 'YYYYMMDD')::varchar
                        end     as dtp_472_date_end,
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_472_PREFIX',
                        'DTP_472_QUALIFIER',
                        'DTP_472_FORMAT',
                        'DTP_472_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_472_PREFIX,
                    DTP_472_QUALIFIER,
                    DTP_472_FORMAT,
                    DTP_472_DATE
                )
)
, ref_d9 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_D9_PREFIX'
                            when    flattened.index = 2   then      'REF_D9_REF_ID_QUALIFIER'
                            when    flattened.index = 3   then      'REF_D9_REF_ID'
                            when    flattened.index = 4   then      'REF_D9_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^REF\\*D9.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'REF_D9_PREFIX',
                        'REF_D9_REF_ID_QUALIFIER',
                        'REF_D9_REF_ID',
                        'REF_D9_DESCRIPTION'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    REF_D9_PREFIX,
                    REF_D9_REF_ID_QUALIFIER,
                    REF_D9_REF_ID,
                    REF_D9_DESCRIPTION
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_19_prefix,
            hl.hl_19_id,
            hl.hl_19_parent_id,
            hl.hl_19_level_code,
            hl.hl_19_child_code,
            nm1_QC.nm1_QC_patient_name_code,
            nm1_QC.nm1_QC_patient_entity_identifier_code,
            nm1_QC.nm1_QC_patient_entity_type_qualifier,
            nm1_QC.nm1_QC_patient_last_name_org,
            nm1_QC.nm1_QC_patient_first_name,
            nm1_QC.nm1_QC_patient_middle_name,
            nm1_QC.nm1_QC_patient_name_prefix,
            nm1_QC.nm1_QC_patient_name_suffix,
            nm1_QC.nm1_QC_patient_id_code_qualifier,
            nm1_QC.nm1_QC_patient_id_code,
            trn.trn_prefix,
            trn.trn_trace_type,
            trn.trn_trace_id, --claim_id
            trn.trn_originating_company_identifier,
            trn.trn_ref_id,
            stc.stc_prefix,
            stc.stc_status_category_code,
            stc.stc_date,
            stc.stc_action_code,
            stc.stc_monetary_amount,
            stc.stc_payment_method,
            stc.stc_date2,
            stc.stc_check_number,
            stc.stc_remit_date,
            dtp_472.dtp_472_prefix,
            dtp_472.dtp_472_qualifier,
            dtp_472.dtp_472_format,
            dtp_472.dtp_472_date,
            dtp_472_date_start,
            dtp_472_date_end,
            ref_d9.ref_d9_prefix,
            ref_d9.ref_d9_ref_id_qualifier,
            ref_d9.ref_d9_ref_id,
            ref_d9.ref_d9_description
from        hl
            left join
                nm1_QC
                on  hl.response_id          = nm1_QC.response_id
                and hl.nth_functional_group = nm1_QC.nth_functional_group
                and hl.nth_transaction_set  = nm1_QC.nth_transaction_set
                and hl.hl_index             = nm1_QC.hl_index
            left join
                trn
                on  hl.response_id          = trn.response_id
                and hl.nth_functional_group = trn.nth_functional_group
                and hl.nth_transaction_set  = trn.nth_transaction_set
                and hl.hl_index             = trn.hl_index
            left join
                stc
                on  hl.response_id          = stc.response_id
                and hl.nth_functional_group = stc.nth_functional_group
                and hl.nth_transaction_set  = stc.nth_transaction_set
                and hl.hl_index             = stc.hl_index
            left join
                dtp_472
                on  hl.response_id          = dtp_472.response_id
                and hl.nth_functional_group = dtp_472.nth_functional_group
                and hl.nth_transaction_set  = dtp_472.nth_transaction_set
                and hl.hl_index             = dtp_472.hl_index
            left join
                ref_d9
                on  hl.response_id          = ref_d9.response_id
                and hl.nth_functional_group = ref_d9.nth_functional_group
                and hl.nth_transaction_set  = ref_d9.nth_transaction_set
                and hl.hl_index             = ref_d9.hl_index
;



create or replace task
    edwprodhh.edi_277_parser.insert_hl_pt_patient
    warehouse = analysis_wh
    after edwprodhh.edi_277_parser.insert_response_flat
as
insert into
    edwprodhh.edi_277_parser.hl_pt_patient
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    HL_INDEX,
    HL_19_PREFIX,
    HL_19_ID,
    HL_19_PARENT_ID,
    HL_19_LEVEL_CODE,
    HL_19_CHILD_CODE,
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
    TRN_PREFIX,
    TRN_TRACE_TYPE,
    TRN_TRACE_ID,
    TRN_ORIGINATING_COMPANY_IDENTIFIER,
    TRN_REF_ID,
    STC_PREFIX,
    STC_STATUS_CATEGORY_CODE,
    STC_DATE,
    STC_ACTION_CODE,
    STC_MONETARY_AMOUNT,
    STC_PAYMENT_METHOD,
    STC_DATE2,
    STC_CHECK_NUMBER,
    STC_REMIT_DATE,
    DTP_472_PREFIX,
    DTP_472_QUALIFIER,
    DTP_472_FORMAT,
    DTP_472_DATE,
    DTP_472_DATE_START,
    DTP_472_DATE_END,
    REF_D9_PREFIX,
    REF_D9_REF_ID_QUALIFIER,
    REF_D9_REF_ID,
    REF_D9_DESCRIPTION
)
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = 'PT'
                and response_id not in (select response_id from edwprodhh.edi_277_parser.hl_pt_patient)
)
, hl as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HL_19_PREFIX'
                            when    flattened.index = 2   then      'HL_19_ID'
                            when    flattened.index = 3   then      'HL_19_PARENT_ID'
                            when    flattened.index = 4   then      'HL_19_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_19_CHILD_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^HL.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'HL_19_PREFIX',
                        'HL_19_ID',
                        'HL_19_PARENT_ID',
                        'HL_19_LEVEL_CODE',
                        'HL_19_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_19_PREFIX,
                    HL_19_ID,
                    HL_19_PARENT_ID,
                    HL_19_LEVEL_CODE,
                    HL_19_CHILD_CODE
                )
)
, nm1_QC as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*QC.*')                         --1 Filter
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
                    HL_INDEX,
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
, trn as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRN_PREFIX'
                            when    flattened.index = 2   then      'TRN_TRACE_TYPE'
                            when    flattened.index = 3   then      'TRN_TRACE_ID'
                            when    flattened.index = 4   then      'TRN_ORIGINATING_COMPANY_IDENTIFIER'
                            when    flattened.index = 5   then      'TRN_REF_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^TRN.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRN_PREFIX',
                        'TRN_TRACE_TYPE',
                        'TRN_TRACE_ID',
                        'TRN_ORIGINATING_COMPANY_IDENTIFIER',
                        'TRN_REF_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    TRN_PREFIX,
                    TRN_TRACE_TYPE,
                    TRN_TRACE_ID,
                    TRN_ORIGINATING_COMPANY_IDENTIFIER,
                    TRN_REF_ID
                )
)
, stc as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'STC_PREFIX'
                            when    flattened.index = 2   then      'STC_STATUS_CATEGORY_CODE'
                            when    flattened.index = 3   then      'STC_DATE'
                            when    flattened.index = 4   then      'STC_ACTION_CODE'
                            when    flattened.index = 5   then      'STC_MONETARY_AMOUNT'
                            when    flattened.index = 6   then      'STC_PAYMENT_METHOD'
                            when    flattened.index = 7   then      'STC_DATE2'
                            when    flattened.index = 8   then      'STC_CHECK_NUMBER'
                            when    flattened.index = 9   then      'STC_REMIT_DATE'
                            end     as value_header,

                    case    when    value_header = 'STC_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^STC.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'STC_PREFIX',
                        'STC_STATUS_CATEGORY_CODE',
                        'STC_DATE',
                        'STC_ACTION_CODE',
                        'STC_MONETARY_AMOUNT',
                        'STC_PAYMENT_METHOD',
                        'STC_DATE2',
                        'STC_CHECK_NUMBER',
                        'STC_REMIT_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    STC_PREFIX,
                    STC_STATUS_CATEGORY_CODE,
                    STC_DATE,
                    STC_ACTION_CODE,
                    STC_MONETARY_AMOUNT,
                    STC_PAYMENT_METHOD,
                    STC_DATE2,
                    STC_CHECK_NUMBER,
                    STC_REMIT_DATE
                )
)
, dtp_472 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_472_PREFIX'
                            when    flattened.index = 2   then      'DTP_472_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_472_FORMAT'
                            when    flattened.index = 4   then      'DTP_472_DATE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*472.*')                          --1 Filter
    )
    select      *,

                case    when    regexp_like(dtp_472_date, '^\\d{8}$')
                        then    to_date(dtp_472_date, 'YYYYMMDD')::varchar
                        when    regexp_like(dtp_472_date, '^\\d{8}\\-\\d{8}$')
                        then    to_date(regexp_substr(dtp_472_date, '^(\\d{8})'), 'YYYYMMDD')::varchar
                        end     as dtp_472_date_start,

                case    when    regexp_like(dtp_472_date, '^\\d{8}$')
                        then    to_date(dtp_472_date, 'YYYYMMDD')::varchar
                        when    regexp_like(dtp_472_date, '^\\d{8}\\-\\d{8}$')
                        then    to_date(regexp_substr(dtp_472_date, '(\\d{8})$'), 'YYYYMMDD')::varchar
                        end     as dtp_472_date_end,
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_472_PREFIX',
                        'DTP_472_QUALIFIER',
                        'DTP_472_FORMAT',
                        'DTP_472_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_472_PREFIX,
                    DTP_472_QUALIFIER,
                    DTP_472_FORMAT,
                    DTP_472_DATE
                )
)
, ref_d9 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_D9_PREFIX'
                            when    flattened.index = 2   then      'REF_D9_REF_ID_QUALIFIER'
                            when    flattened.index = 3   then      'REF_D9_REF_ID'
                            when    flattened.index = 4   then      'REF_D9_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^REF\\*D9.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'REF_D9_PREFIX',
                        'REF_D9_REF_ID_QUALIFIER',
                        'REF_D9_REF_ID',
                        'REF_D9_DESCRIPTION'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    REF_D9_PREFIX,
                    REF_D9_REF_ID_QUALIFIER,
                    REF_D9_REF_ID,
                    REF_D9_DESCRIPTION
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_19_prefix,
            hl.hl_19_id,
            hl.hl_19_parent_id,
            hl.hl_19_level_code,
            hl.hl_19_child_code,
            nm1_QC.nm1_QC_patient_name_code,
            nm1_QC.nm1_QC_patient_entity_identifier_code,
            nm1_QC.nm1_QC_patient_entity_type_qualifier,
            nm1_QC.nm1_QC_patient_last_name_org,
            nm1_QC.nm1_QC_patient_first_name,
            nm1_QC.nm1_QC_patient_middle_name,
            nm1_QC.nm1_QC_patient_name_prefix,
            nm1_QC.nm1_QC_patient_name_suffix,
            nm1_QC.nm1_QC_patient_id_code_qualifier,
            nm1_QC.nm1_QC_patient_id_code,
            trn.trn_prefix,
            trn.trn_trace_type,
            trn.trn_trace_id, --claim_id
            trn.trn_originating_company_identifier,
            trn.trn_ref_id,
            stc.stc_prefix,
            stc.stc_status_category_code,
            stc.stc_date,
            stc.stc_action_code,
            stc.stc_monetary_amount,
            stc.stc_payment_method,
            stc.stc_date2,
            stc.stc_check_number,
            stc.stc_remit_date,
            dtp_472.dtp_472_prefix,
            dtp_472.dtp_472_qualifier,
            dtp_472.dtp_472_format,
            dtp_472.dtp_472_date,
            dtp_472_date_start,
            dtp_472_date_end,
            ref_d9.ref_d9_prefix,
            ref_d9.ref_d9_ref_id_qualifier,
            ref_d9.ref_d9_ref_id,
            ref_d9.ref_d9_description
from        hl
            left join
                nm1_QC
                on  hl.response_id          = nm1_QC.response_id
                and hl.nth_functional_group = nm1_QC.nth_functional_group
                and hl.nth_transaction_set  = nm1_QC.nth_transaction_set
                and hl.hl_index             = nm1_QC.hl_index
            left join
                trn
                on  hl.response_id          = trn.response_id
                and hl.nth_functional_group = trn.nth_functional_group
                and hl.nth_transaction_set  = trn.nth_transaction_set
                and hl.hl_index             = trn.hl_index
            left join
                stc
                on  hl.response_id          = stc.response_id
                and hl.nth_functional_group = stc.nth_functional_group
                and hl.nth_transaction_set  = stc.nth_transaction_set
                and hl.hl_index             = stc.hl_index
            left join
                dtp_472
                on  hl.response_id          = dtp_472.response_id
                and hl.nth_functional_group = dtp_472.nth_functional_group
                and hl.nth_transaction_set  = dtp_472.nth_transaction_set
                and hl.hl_index             = dtp_472.hl_index
            left join
                ref_d9
                on  hl.response_id          = ref_d9.response_id
                and hl.nth_functional_group = ref_d9.nth_functional_group
                and hl.nth_transaction_set  = ref_d9.nth_transaction_set
                and hl.hl_index             = ref_d9.hl_index
;