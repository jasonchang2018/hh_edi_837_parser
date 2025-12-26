create or replace table
    edwprodhh.edi_277_parser.hl_20_payer
as
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = '20'
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

                    case    when    flattened.index = 1   then      'HL_20_PREFIX'
                            when    flattened.index = 2   then      'HL_20_ID'
                            when    flattened.index = 3   then      'HL_20_PARENT_ID'
                            when    flattened.index = 4   then      'HL_20_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_20_CHILD_CODE'
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
                        'HL_20_PREFIX',
                        'HL_20_ID',
                        'HL_20_PARENT_ID',
                        'HL_20_LEVEL_CODE',
                        'HL_20_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_20_PREFIX,
                    HL_20_ID,
                    HL_20_PARENT_ID,
                    HL_20_LEVEL_CODE,
                    HL_20_CHILD_CODE
                )
)
, nm1_PR as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_PR_PATIENT_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_PR_PATIENT_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_PR_PATIENT_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_PR_PATIENT_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_PR_PATIENT_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_PR_PATIENT_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_PR_PATIENT_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_PR_PATIENT_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_PR_PATIENT_NAME_CODE',
                        'NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE',
                        'NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER',
                        'NM1_PR_PATIENT_LAST_NAME_ORG',
                        'NM1_PR_PATIENT_FIRST_NAME',
                        'NM1_PR_PATIENT_MIDDLE_NAME',
                        'NM1_PR_PATIENT_NAME_PREFIX',
                        'NM1_PR_PATIENT_NAME_SUFFIX',
                        'NM1_PR_PATIENT_ID_CODE_QUALIFIER',
                        'NM1_PR_PATIENT_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    NM1_PR_PATIENT_NAME_CODE,
                    NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE,
                    NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER,
                    NM1_PR_PATIENT_LAST_NAME_ORG,
                    NM1_PR_PATIENT_FIRST_NAME,
                    NM1_PR_PATIENT_MIDDLE_NAME,
                    NM1_PR_PATIENT_NAME_PREFIX,
                    NM1_PR_PATIENT_NAME_SUFFIX,
                    NM1_PR_PATIENT_ID_CODE_QUALIFIER,
                    NM1_PR_PATIENT_ID_CODE
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
, dtp_050 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_050_PREFIX'
                            when    flattened.index = 2   then      'DTP_050_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_050_FORMAT'
                            when    flattened.index = 4   then      'DTP_050_DATE'
                            end     as value_header,

                    case    when    value_header = 'DTP_050_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*050.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_050_PREFIX',
                        'DTP_050_QUALIFIER',
                        'DTP_050_FORMAT',
                        'DTP_050_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_050_PREFIX,
                    DTP_050_QUALIFIER,
                    DTP_050_FORMAT,
                    DTP_050_DATE
                )
)
, dtp_009 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_009_PREFIX'
                            when    flattened.index = 2   then      'DTP_009_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_009_FORMAT'
                            when    flattened.index = 4   then      'DTP_009_DATE'
                            end     as value_header,

                    case    when    value_header = 'DTP_009_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*009.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_009_PREFIX',
                        'DTP_009_QUALIFIER',
                        'DTP_009_FORMAT',
                        'DTP_009_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_009_PREFIX,
                    DTP_009_QUALIFIER,
                    DTP_009_FORMAT,
                    DTP_009_DATE
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_20_prefix,
            hl.hl_20_id,
            hl.hl_20_parent_id,
            hl.hl_20_level_code,
            hl.hl_20_child_code,
            nm1_pr.nm1_pr_patient_name_code,
            nm1_pr.nm1_pr_patient_entity_identifier_code,
            nm1_pr.nm1_pr_patient_entity_type_qualifier,
            nm1_pr.nm1_pr_patient_last_name_org,
            nm1_pr.nm1_pr_patient_first_name,
            nm1_pr.nm1_pr_patient_middle_name,
            nm1_pr.nm1_pr_patient_name_prefix,
            nm1_pr.nm1_pr_patient_name_suffix,
            nm1_pr.nm1_pr_patient_id_code_qualifier,
            nm1_pr.nm1_pr_patient_id_code,
            trn.trn_prefix,
            trn.trn_trace_type,
            trn.trn_trace_id,
            trn.trn_originating_company_identifier,
            trn.trn_ref_id,
            dtp_050.dtp_050_prefix,
            dtp_050.dtp_050_qualifier,
            dtp_050.dtp_050_format,
            dtp_050.dtp_050_date,
            dtp_009.dtp_009_prefix,
            dtp_009.dtp_009_qualifier,
            dtp_009.dtp_009_format,
            dtp_009.dtp_009_date
from        hl
            left join
                nm1_PR
                on  hl.response_id          = nm1_PR.response_id
                and hl.nth_functional_group = nm1_PR.nth_functional_group
                and hl.nth_transaction_set  = nm1_PR.nth_transaction_set
                and hl.hl_index             = nm1_PR.hl_index
            left join
                trn
                on  hl.response_id          = trn.response_id
                and hl.nth_functional_group = trn.nth_functional_group
                and hl.nth_transaction_set  = trn.nth_transaction_set
                and hl.hl_index             = trn.hl_index
            left join
                dtp_050
                on  hl.response_id          = dtp_050.response_id
                and hl.nth_functional_group = dtp_050.nth_functional_group
                and hl.nth_transaction_set  = dtp_050.nth_transaction_set
                and hl.hl_index             = dtp_050.hl_index
            left join
                dtp_009
                on  hl.response_id          = dtp_009.response_id
                and hl.nth_functional_group = dtp_009.nth_functional_group
                and hl.nth_transaction_set  = dtp_009.nth_transaction_set
                and hl.hl_index             = dtp_009.hl_index
;



create or replace task
    edwprodhh.edi_277_parser.insert_hl_20_payer
    warehouse = analysis_wh
    after edwprodhh.edi_277_parser.insert_response_flat
as
insert into
    edwprodhh.edi_277_parser.hl_20_payer
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    HL_INDEX,
    HL_20_PREFIX,
    HL_20_ID,
    HL_20_PARENT_ID,
    HL_20_LEVEL_CODE,
    HL_20_CHILD_CODE,
    NM1_PR_PATIENT_NAME_CODE,
    NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE,
    NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER,
    NM1_PR_PATIENT_LAST_NAME_ORG,
    NM1_PR_PATIENT_FIRST_NAME,
    NM1_PR_PATIENT_MIDDLE_NAME,
    NM1_PR_PATIENT_NAME_PREFIX,
    NM1_PR_PATIENT_NAME_SUFFIX,
    NM1_PR_PATIENT_ID_CODE_QUALIFIER,
    NM1_PR_PATIENT_ID_CODE,
    TRN_PREFIX,
    TRN_TRACE_TYPE,
    TRN_TRACE_ID,
    TRN_ORIGINATING_COMPANY_IDENTIFIER,
    TRN_REF_ID,
    DTP_050_PREFIX,
    DTP_050_QUALIFIER,
    DTP_050_FORMAT,
    DTP_050_DATE,
    DTP_009_PREFIX,
    DTP_009_QUALIFIER,
    DTP_009_FORMAT,
    DTP_009_DATE
)
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = '20'
                and response_id not in (select response_id from edwprodhh.edi_277_parser.hl_20_payer)
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

                    case    when    flattened.index = 1   then      'HL_20_PREFIX'
                            when    flattened.index = 2   then      'HL_20_ID'
                            when    flattened.index = 3   then      'HL_20_PARENT_ID'
                            when    flattened.index = 4   then      'HL_20_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_20_CHILD_CODE'
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
                        'HL_20_PREFIX',
                        'HL_20_ID',
                        'HL_20_PARENT_ID',
                        'HL_20_LEVEL_CODE',
                        'HL_20_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_20_PREFIX,
                    HL_20_ID,
                    HL_20_PARENT_ID,
                    HL_20_LEVEL_CODE,
                    HL_20_CHILD_CODE
                )
)
, nm1_PR as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_PR_PATIENT_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_PR_PATIENT_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_PR_PATIENT_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_PR_PATIENT_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_PR_PATIENT_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_PR_PATIENT_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_PR_PATIENT_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_PR_PATIENT_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*PR.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_PR_PATIENT_NAME_CODE',
                        'NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE',
                        'NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER',
                        'NM1_PR_PATIENT_LAST_NAME_ORG',
                        'NM1_PR_PATIENT_FIRST_NAME',
                        'NM1_PR_PATIENT_MIDDLE_NAME',
                        'NM1_PR_PATIENT_NAME_PREFIX',
                        'NM1_PR_PATIENT_NAME_SUFFIX',
                        'NM1_PR_PATIENT_ID_CODE_QUALIFIER',
                        'NM1_PR_PATIENT_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    NM1_PR_PATIENT_NAME_CODE,
                    NM1_PR_PATIENT_ENTITY_IDENTIFIER_CODE,
                    NM1_PR_PATIENT_ENTITY_TYPE_QUALIFIER,
                    NM1_PR_PATIENT_LAST_NAME_ORG,
                    NM1_PR_PATIENT_FIRST_NAME,
                    NM1_PR_PATIENT_MIDDLE_NAME,
                    NM1_PR_PATIENT_NAME_PREFIX,
                    NM1_PR_PATIENT_NAME_SUFFIX,
                    NM1_PR_PATIENT_ID_CODE_QUALIFIER,
                    NM1_PR_PATIENT_ID_CODE
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
, dtp_050 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_050_PREFIX'
                            when    flattened.index = 2   then      'DTP_050_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_050_FORMAT'
                            when    flattened.index = 4   then      'DTP_050_DATE'
                            end     as value_header,

                    case    when    value_header = 'DTP_050_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*050.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_050_PREFIX',
                        'DTP_050_QUALIFIER',
                        'DTP_050_FORMAT',
                        'DTP_050_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_050_PREFIX,
                    DTP_050_QUALIFIER,
                    DTP_050_FORMAT,
                    DTP_050_DATE
                )
)
, dtp_009 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_009_PREFIX'
                            when    flattened.index = 2   then      'DTP_009_QUALIFIER'
                            when    flattened.index = 3   then      'DTP_009_FORMAT'
                            when    flattened.index = 4   then      'DTP_009_DATE'
                            end     as value_header,

                    case    when    value_header = 'DTP_009_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened      --2 Flatten

        where       regexp_like(filtered.line_element_277, '^DTP\\*009.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_009_PREFIX',
                        'DTP_009_QUALIFIER',
                        'DTP_009_FORMAT',
                        'DTP_009_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    DTP_009_PREFIX,
                    DTP_009_QUALIFIER,
                    DTP_009_FORMAT,
                    DTP_009_DATE
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_20_prefix,
            hl.hl_20_id,
            hl.hl_20_parent_id,
            hl.hl_20_level_code,
            hl.hl_20_child_code,
            nm1_pr.nm1_pr_patient_name_code,
            nm1_pr.nm1_pr_patient_entity_identifier_code,
            nm1_pr.nm1_pr_patient_entity_type_qualifier,
            nm1_pr.nm1_pr_patient_last_name_org,
            nm1_pr.nm1_pr_patient_first_name,
            nm1_pr.nm1_pr_patient_middle_name,
            nm1_pr.nm1_pr_patient_name_prefix,
            nm1_pr.nm1_pr_patient_name_suffix,
            nm1_pr.nm1_pr_patient_id_code_qualifier,
            nm1_pr.nm1_pr_patient_id_code,
            trn.trn_prefix,
            trn.trn_trace_type,
            trn.trn_trace_id,
            trn.trn_originating_company_identifier,
            trn.trn_ref_id,
            dtp_050.dtp_050_prefix,
            dtp_050.dtp_050_qualifier,
            dtp_050.dtp_050_format,
            dtp_050.dtp_050_date,
            dtp_009.dtp_009_prefix,
            dtp_009.dtp_009_qualifier,
            dtp_009.dtp_009_format,
            dtp_009.dtp_009_date
from        hl
            left join
                nm1_PR
                on  hl.response_id          = nm1_PR.response_id
                and hl.nth_functional_group = nm1_PR.nth_functional_group
                and hl.nth_transaction_set  = nm1_PR.nth_transaction_set
                and hl.hl_index             = nm1_PR.hl_index
            left join
                trn
                on  hl.response_id          = trn.response_id
                and hl.nth_functional_group = trn.nth_functional_group
                and hl.nth_transaction_set  = trn.nth_transaction_set
                and hl.hl_index             = trn.hl_index
            left join
                dtp_050
                on  hl.response_id          = dtp_050.response_id
                and hl.nth_functional_group = dtp_050.nth_functional_group
                and hl.nth_transaction_set  = dtp_050.nth_transaction_set
                and hl.hl_index             = dtp_050.hl_index
            left join
                dtp_009
                on  hl.response_id          = dtp_009.response_id
                and hl.nth_functional_group = dtp_009.nth_functional_group
                and hl.nth_transaction_set  = dtp_009.nth_transaction_set
                and hl.hl_index             = dtp_009.hl_index
;