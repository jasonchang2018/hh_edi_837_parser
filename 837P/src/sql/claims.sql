create or replace table
    edwprodhh.edi_837p_parser.claims
as
with filtered_clm as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is null
)
, header_clm as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_PREFIX'
                            when    flattened.index = 2   then      'CLAIM_ID'
                            when    flattened.index = 3   then      'TOTAL_CLAIM_CHARGE'
                            when    flattened.index = 4   then      'PATIENT_CONTROL_NUMBER'
                            when    flattened.index = 5   then      'FACILITY_CODE_VALUE'
                            when    flattened.index = 6   then      'PLACE_OF_SERVICE'           --11/13/21 Office/Hospital/Inpatient : ...
                            when    flattened.index = 7   then      'PROVIDER_SIGNATURE_ON_FILE'
                            when    flattened.index = 8   then      'ASSIGNMENT_PLAN_PARTICIPATION'
                            when    flattened.index = 9   then      'BENEFITS_ASSIGNMENT_INDICATOR'
                            when    flattened.index = 10  then      'RELEASE_OF_INFO_CODE'
                            when    flattened.index = 11  then      'CLAIM_11'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CLM.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_PREFIX',
                        'CLAIM_ID',
                        'TOTAL_CLAIM_CHARGE',
                        'PATIENT_CONTROL_NUMBER',
                        'FACILITY_CODE_VALUE',
                        'PLACE_OF_SERVICE',
                        'PROVIDER_SIGNATURE_ON_FILE',
                        'ASSIGNMENT_PLAN_PARTICIPATION',
                        'BENEFITS_ASSIGNMENT_INDICATOR',
                        'RELEASE_OF_INFO_CODE',
                        'CLAIM_11'
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
                    CLM_PREFIX,
                    CLAIM_ID,
                    TOTAL_CLAIM_CHARGE,
                    PATIENT_CONTROL_NUMBER,
                    FACILITY_CODE_VALUE,
                    PLACE_OF_SERVICE,
                    PROVIDER_SIGNATURE_ON_FILE,
                    ASSIGNMENT_PLAN_PARTICIPATION,
                    BENEFITS_ASSIGNMENT_INDICATOR,
                    RELEASE_OF_INFO_CODE,
                    CLAIM_11
                )
)
, claim_dtp_435 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_ADMIT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_ADMIT'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_ADMIT'
                            when    flattened.index = 4   then      'DATETIME_CLAIM_ADMIT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*435.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    to_date(left(datetime_claim_admit,  8), 'YYYYMMDD')
                        else    NULL
                        end     as admit_date_claim,

                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    left(right(datetime_claim_admit, 4), 2)
                        else    NULL
                        end     as admit_hour_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_ADMIT',
                        'DATE_QUALIFIER_CLAIM_ADMIT',
                        'DATE_FORMAT_CLAIM_ADMIT',
                        'DATETIME_CLAIM_ADMIT'
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
                    DTP_PREFIX_CLAIM_ADMIT,
                    DATE_QUALIFIER_CLAIM_ADMIT,
                    DATE_FORMAT_CLAIM_ADMIT,
                    DATETIME_CLAIM_ADMIT
                )
)
, claim_dtp_439 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_ACCIDENT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_ACCIDENT'
                            when    flattened.index = 3   then      'DATE_FORMAT_ACCIDENT'
                            when    flattened.index = 4   then      'DATE_RANGE_ACCIDENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*434.*')                          --1 Filter
    )
    select      *
                -- case    when    date_format_claim = 'RD8'
                --         and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                --         then    to_date(left(date_range_claim,  8), 'YYYYMMDD')
                --         else    NULL
                --         end     as start_date_claim,

                -- case    when    date_format_claim = 'RD8'
                --         and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                --         then    to_date(right(date_range_claim, 8), 'YYYYMMDD')
                --         else    NULL
                --         end     as end_date_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_ACCIDENT',
                        'DATE_QUALIFIER_ACCIDENT',
                        'DATE_FORMAT_ACCIDENT',
                        'DATE_RANGE_ACCIDENT'
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
                    DTP_PREFIX_ACCIDENT,
                    DATE_QUALIFIER_ACCIDENT,
                    DATE_FORMAT_ACCIDENT,
                    DATE_RANGE_ACCIDENT
                )
)
, claim_dtp_096 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_DISCHARGE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_DISCHARGE'
                            when    flattened.index = 3   then      'DATE_FORMAT_DISCHARGE'
                            when    flattened.index = 4   then      'TIME_DISCHARGE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*096.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_DISCHARGE',
                        'DATE_QUALIFIER_DISCHARGE',
                        'DATE_FORMAT_DISCHARGE',
                        'TIME_DISCHARGE'
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
                    DTP_PREFIX_DISCHARGE,
                    DATE_QUALIFIER_DISCHARGE,
                    DATE_FORMAT_DISCHARGE,
                    TIME_DISCHARGE
                )
)
, claim_dtp_484 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_BILLING_PERIOD'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_BILLING_PERIOD'
                            when    flattened.index = 3   then      'DATE_FORMAT_BILLING_PERIOD'
                            when    flattened.index = 4   then      'TIME_BILLING_PERIOD'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*484.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_BILLING_PERIOD',
                        'DATE_QUALIFIER_BILLING_PERIOD',
                        'DATE_FORMAT_BILLING_PERIOD',
                        'TIME_BILLING_PERIOD'
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
                    DTP_PREFIX_BILLING_PERIOD,
                    DATE_QUALIFIER_BILLING_PERIOD,
                    DATE_FORMAT_BILLING_PERIOD,
                    TIME_BILLING_PERIOD
                )
)
, claim_nm82 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_RENDERING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_RENDERING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_RENDERING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_RENDERING'
                            when    flattened.index = 5   then      'FIRST_NAME_RENDERING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_RENDERING'
                            when    flattened.index = 7   then      'NAME_PREFIX_RENDERING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_RENDERING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_RENDERING'
                            when    flattened.index = 10  then      'ID_CODE_RENDERING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*82.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_RENDERING',
                        'ENTITY_IDENTIFIER_CODE_RENDERING',
                        'ENTITY_TYPE_QUALIFIER_RENDERING',
                        'LAST_NAME_ORG_RENDERING',
                        'FIRST_NAME_RENDERING',
                        'MIDDLE_NAME_RENDERING',
                        'NAME_PREFIX_RENDERING',
                        'NAME_SUFFIX_RENDERING',
                        'ID_CODE_QUALIFIER_RENDERING',
                        'ID_CODE_RENDERING'
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
                    NAME_CODE_RENDERING,
                    ENTITY_IDENTIFIER_CODE_RENDERING,
                    ENTITY_TYPE_QUALIFIER_RENDERING,
                    LAST_NAME_ORG_RENDERING,
                    FIRST_NAME_RENDERING,
                    MIDDLE_NAME_RENDERING,
                    NAME_PREFIX_RENDERING,
                    NAME_SUFFIX_RENDERING,
                    ID_CODE_QUALIFIER_RENDERING,
                    ID_CODE_RENDERING
                )
)
, claim_nm82_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_RENDERING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_RENDERING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_RENDERING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_RENDERING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*82'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_RENDERING',
                        'PROVIDER_CODE_RENDERING',
                        'REFERENCE_ID_QUALIFIER_RENDERING',
                        'PROVIDER_TAXONOMY_CODE_RENDERING'
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
                    PRV_PREFIX_RENDERING,
                    PROVIDER_CODE_RENDERING,
                    REFERENCE_ID_QUALIFIER_RENDERING,
                    PROVIDER_TAXONOMY_CODE_RENDERING
                )
)
, claim_nm77 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_SERVICE_LOC'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SERVICE_LOC'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SERVICE_LOC'
                            when    flattened.index = 5   then      'FIRST_NAME_SERVICE_LOC'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SERVICE_LOC'
                            when    flattened.index = 7   then      'NAME_PREFIX_SERVICE_LOC'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SERVICE_LOC'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 10  then      'ID_CODE_SERVICE_LOC'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*77.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SERVICE_LOC',
                        'ENTITY_IDENTIFIER_CODE_SERVICE_LOC',
                        'ENTITY_TYPE_QUALIFIER_SERVICE_LOC',
                        'LAST_NAME_ORG_SERVICE_LOC',
                        'FIRST_NAME_SERVICE_LOC',
                        'MIDDLE_NAME_SERVICE_LOC',
                        'NAME_PREFIX_SERVICE_LOC',
                        'NAME_SUFFIX_SERVICE_LOC',
                        'ID_CODE_QUALIFIER_SERVICE_LOC',
                        'ID_CODE_SERVICE_LOC'
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
                    NAME_CODE_SERVICE_LOC,
                    ENTITY_IDENTIFIER_CODE_SERVICE_LOC,
                    ENTITY_TYPE_QUALIFIER_SERVICE_LOC,
                    LAST_NAME_ORG_SERVICE_LOC,
                    FIRST_NAME_SERVICE_LOC,
                    MIDDLE_NAME_SERVICE_LOC,
                    NAME_PREFIX_SERVICE_LOC,
                    NAME_SUFFIX_SERVICE_LOC,
                    ID_CODE_QUALIFIER_SERVICE_LOC,
                    ID_CODE_SERVICE_LOC
                )
)
, claim_nm77_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_SERVICE_LOC'
                            when    flattened.index = 2   then      'PROVIDER_CODE_SERVICE_LOC'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_SERVICE_LOC'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*77'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_SERVICE_LOC',
                        'PROVIDER_CODE_SERVICE_LOC',
                        'REFERENCE_ID_QUALIFIER_SERVICE_LOC',
                        'PROVIDER_TAXONOMY_CODE_SERVICE_LOC'
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
                    PRV_PREFIX_SERVICE_LOC,
                    PROVIDER_CODE_SERVICE_LOC,
                    REFERENCE_ID_QUALIFIER_SERVICE_LOC,
                    PROVIDER_TAXONOMY_CODE_SERVICE_LOC
                )
)
, claim_nmDN as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_REFERRING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_REFERRING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_REFERRING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_REFERRING'
                            when    flattened.index = 5   then      'FIRST_NAME_REFERRING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_REFERRING'
                            when    flattened.index = 7   then      'NAME_PREFIX_REFERRING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_REFERRING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_REFERRING'
                            when    flattened.index = 10  then      'ID_CODE_REFERRING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*DN.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_REFERRING',
                        'ENTITY_IDENTIFIER_CODE_REFERRING',
                        'ENTITY_TYPE_QUALIFIER_REFERRING',
                        'LAST_NAME_ORG_REFERRING',
                        'FIRST_NAME_REFERRING',
                        'MIDDLE_NAME_REFERRING',
                        'NAME_PREFIX_REFERRING',
                        'NAME_SUFFIX_REFERRING',
                        'ID_CODE_QUALIFIER_REFERRING',
                        'ID_CODE_REFERRING'
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
                    NAME_CODE_REFERRING,
                    ENTITY_IDENTIFIER_CODE_REFERRING,
                    ENTITY_TYPE_QUALIFIER_REFERRING,
                    LAST_NAME_ORG_REFERRING,
                    FIRST_NAME_REFERRING,
                    MIDDLE_NAME_REFERRING,
                    NAME_PREFIX_REFERRING,
                    NAME_SUFFIX_REFERRING,
                    ID_CODE_QUALIFIER_REFERRING,
                    ID_CODE_REFERRING
                )
)
, claim_nmPW as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PRIOR_PAYER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PRIOR_PAYER'
                            when    flattened.index = 5   then      'FIRST_NAME_PRIOR_PAYER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PRIOR_PAYER'
                            when    flattened.index = 7   then      'NAME_PREFIX_PRIOR_PAYER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PRIOR_PAYER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 10  then      'ID_CODE_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*PW.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PRIOR_PAYER',
                        'ENTITY_IDENTIFIER_CODE_PRIOR_PAYER',
                        'ENTITY_TYPE_QUALIFIER_PRIOR_PAYER',
                        'LAST_NAME_ORG_PRIOR_PAYER',
                        'FIRST_NAME_PRIOR_PAYER',
                        'MIDDLE_NAME_PRIOR_PAYER',
                        'NAME_PREFIX_PRIOR_PAYER',
                        'NAME_SUFFIX_PRIOR_PAYER',
                        'ID_CODE_QUALIFIER_PRIOR_PAYER',
                        'ID_CODE_PRIOR_PAYER'
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
                    NAME_CODE_PRIOR_PAYER,
                    ENTITY_IDENTIFIER_CODE_PRIOR_PAYER,
                    ENTITY_TYPE_QUALIFIER_PRIOR_PAYER,
                    LAST_NAME_ORG_PRIOR_PAYER,
                    FIRST_NAME_PRIOR_PAYER,
                    MIDDLE_NAME_PRIOR_PAYER,
                    NAME_PREFIX_PRIOR_PAYER,
                    NAME_SUFFIX_PRIOR_PAYER,
                    ID_CODE_QUALIFIER_PRIOR_PAYER,
                    ID_CODE_PRIOR_PAYER
                )
)
, claim_nmPW_n3 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PRIOR_PAYER_N3'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N3.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*PW'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PRIOR_PAYER_N3',
                        'ADDRESS_LINE_1_PRIOR_PAYER',
                        'ADDRESS_LINE_2_PRIOR_PAYER'
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
                    ADDRESS_CODE_PRIOR_PAYER_N3,
                    ADDRESS_LINE_1_PRIOR_PAYER,
                    ADDRESS_LINE_2_PRIOR_PAYER
                )
)
, claim_nmPW_n4 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PRIOR_PAYER_N4'
                            when    flattened.index = 2   then      'CITY_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ST_PRIOR_PAYER'
                            when    flattened.index = 4   then      'ZIP_PRIOR_PAYER'
                            when    flattened.index = 5   then      'COUNTRY_PRIOR_PAYER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N4.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*PW'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PRIOR_PAYER_N4',
                        'CITY_PRIOR_PAYER',
                        'ST_PRIOR_PAYER',
                        'ZIP_PRIOR_PAYER',
                        'COUNTRY_PRIOR_PAYER',
                        'LOCATION_QUALIFIER_PRIOR_PAYER',
                        'LOCATION_IDENTIFIER_PRIOR_PAYER'
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
                    ADDRESS_CODE_PRIOR_PAYER_N4,
                    CITY_PRIOR_PAYER,
                    ST_PRIOR_PAYER,
                    ZIP_PRIOR_PAYER,
                    COUNTRY_PRIOR_PAYER,
                    LOCATION_QUALIFIER_PRIOR_PAYER,
                    LOCATION_IDENTIFIER_PRIOR_PAYER
                )
)
, claim_nm45 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_DROPOFF'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_DROPOFF'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_DROPOFF'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_DROPOFF'
                            when    flattened.index = 5   then      'FIRST_NAME_DROPOFF'
                            when    flattened.index = 6   then      'MIDDLE_NAME_DROPOFF'
                            when    flattened.index = 7   then      'NAME_PREFIX_DROPOFF'
                            when    flattened.index = 8   then      'NAME_SUFFIX_DROPOFF'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_DROPOFF'
                            when    flattened.index = 10  then      'ID_CODE_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*45.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_DROPOFF',
                        'ENTITY_IDENTIFIER_CODE_DROPOFF',
                        'ENTITY_TYPE_QUALIFIER_DROPOFF',
                        'LAST_NAME_ORG_DROPOFF',
                        'FIRST_NAME_DROPOFF',
                        'MIDDLE_NAME_DROPOFF',
                        'NAME_PREFIX_DROPOFF',
                        'NAME_SUFFIX_DROPOFF',
                        'ID_CODE_QUALIFIER_DROPOFF',
                        'ID_CODE_DROPOFF'
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
                    NAME_CODE_DROPOFF,
                    ENTITY_IDENTIFIER_CODE_DROPOFF,
                    ENTITY_TYPE_QUALIFIER_DROPOFF,
                    LAST_NAME_ORG_DROPOFF,
                    FIRST_NAME_DROPOFF,
                    MIDDLE_NAME_DROPOFF,
                    NAME_PREFIX_DROPOFF,
                    NAME_SUFFIX_DROPOFF,
                    ID_CODE_QUALIFIER_DROPOFF,
                    ID_CODE_DROPOFF
                )
)
, claim_nm45_n3 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_DROPOFF_N3'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_DROPOFF'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N3.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*45'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_DROPOFF_N3',
                        'ADDRESS_LINE_1_DROPOFF',
                        'ADDRESS_LINE_2_DROPOFF'
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
                    ADDRESS_CODE_DROPOFF_N3,
                    ADDRESS_LINE_1_DROPOFF,
                    ADDRESS_LINE_2_DROPOFF
                )
)
, claim_nm45_n4 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_DROPOFF_N4'
                            when    flattened.index = 2   then      'CITY_DROPOFF'
                            when    flattened.index = 3   then      'ST_DROPOFF'
                            when    flattened.index = 4   then      'ZIP_DROPOFF'
                            when    flattened.index = 5   then      'COUNTRY_DROPOFF'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_DROPOFF'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N4.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*45'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_DROPOFF_N4',
                        'CITY_DROPOFF',
                        'ST_DROPOFF',
                        'ZIP_DROPOFF',
                        'COUNTRY_DROPOFF',
                        'LOCATION_QUALIFIER_DROPOFF',
                        'LOCATION_IDENTIFIER_DROPOFF'
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
                    ADDRESS_CODE_DROPOFF_N4,
                    CITY_DROPOFF,
                    ST_DROPOFF,
                    ZIP_DROPOFF,
                    COUNTRY_DROPOFF,
                    LOCATION_QUALIFIER_DROPOFF,
                    LOCATION_IDENTIFIER_DROPOFF
                )
)
, claim_pwk as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PWK_PREFIX'
                            when    flattened.index = 2   then      'PWK_REPORT_TYPE_CODE'
                            when    flattened.index = 3   then      'PWK_TRANSMISSION_CODE'
                            when    flattened.index = 4   then      'PWK_COPIES_NEEDED'
                            when    flattened.index = 5   then      'PWK_ENTITY_IDENTIFIER'
                            when    flattened.index = 6   then      'PWK_ID_QUALIFIER'
                            when    flattened.index = 7   then      'PWK_CONTROL_NUMBER_ATTACHMENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PWK.*')                          --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PWK_PREFIX',
                            'PWK_REPORT_TYPE_CODE',
                            'PWK_TRANSMISSION_CODE',
                            'PWK_COPIES_NEEDED',
                            'PWK_ENTITY_IDENTIFIER',
                            'PWK_ID_QUALIFIER',
                            'PWK_CONTROL_NUMBER_ATTACHMENT'
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
                        PWK_PREFIX,
                        PWK_REPORT_TYPE_CODE,
                        PWK_TRANSMISSION_CODE,
                        PWK_COPIES_NEEDED,
                        PWK_ENTITY_IDENTIFIER,
                        PWK_ID_QUALIFIER,
                        PWK_CONTROL_NUMBER_ATTACHMENT
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'pwk_prefix',                       pwk_prefix::varchar,
                        'pwk_report_type_code',             pwk_report_type_code::varchar,
                        'pwk_transmission_code',            pwk_transmission_code::varchar,
                        'pwk_copies_needed',                pwk_copies_needed::varchar,
                        'pwk_entity_identifier',            pwk_entity_identifier::varchar,
                        'pwk_id_qualifier',                 pwk_id_qualifier::varchar,
                        'pwk_control_number_attachment',    pwk_control_number_attachment::varchar
                    )
                )   as clm_pwk_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, claim_nte as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_NTE_PREFIX'
                            when    flattened.index = 2   then      'CLM_NOTE_REF_CODE'
                            when    flattened.index = 3   then      'CLM_NOTE_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NTE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_NTE_PREFIX',
                        'CLM_NOTE_REF_CODE',
                        'CLM_NOTE_DESCRIPTION'
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
                    CLM_NTE_PREFIX,
                    CLM_NOTE_REF_CODE,
                    CLM_NOTE_DESCRIPTION
                )
)
, claim_ref as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'REFERENCE_ID_CODE_CLAIM'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REFERENCE_ID_CLAIM'
                            when    flattened.index = 4   then      'DESCRIPTION_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^REF.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_CLAIM',
                            'REFERENCE_ID_CODE_CLAIM',
                            'REFERENCE_ID_CLAIM',
                            'DESCRIPTION_CLAIM'
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
                        REF_PREFIX_CLAIM,
                        REFERENCE_ID_CODE_CLAIM,
                        REFERENCE_ID_CLAIM,
                        DESCRIPTION_CLAIM
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_code_claim::varchar,
                        'claim_ref_value',          reference_id_claim::varchar,
                        'claim_ref_description',    description_claim::varchar
                    )
                )   as clm_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, claim_hi as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HI_PREFIX'
                            when    flattened.index = 2   then      'HI_VAL01'
                            when    flattened.index = 3   then      'HI_VAL02'
                            when    flattened.index = 4   then      'HI_VAL03'
                            when    flattened.index = 5   then      'HI_VAL04'
                            when    flattened.index = 6   then      'HI_VAL05'
                            when    flattened.index = 7   then      'HI_VAL06'
                            when    flattened.index = 8   then      'HI_VAL07'
                            when    flattened.index = 9   then      'HI_VAL08'
                            when    flattened.index = 10  then      'HI_VAL09'
                            when    flattened.index = 11  then      'HI_VAL10'
                            when    flattened.index = 12  then      'HI_VAL11'
                            when    flattened.index = 13  then      'HI_VAL12'
                            when    flattened.index = 14  then      'HI_VAL13'
                            when    flattened.index = 15  then      'HI_VAL14'
                            when    flattened.index = 16  then      'HI_VAL15'
                            when    flattened.index = 17  then      'HI_VAL16'
                            when    flattened.index = 18  then      'HI_VAL17'
                            when    flattened.index = 19  then      'HI_VAL18'
                            when    flattened.index = 20 then       'HI_VAL19'
                            when    flattened.index = 21  then      'HI_VAL20'
                            when    flattened.index = 22  then      'HI_VAL21'
                            when    flattened.index = 23  then      'HI_VAL22'
                            when    flattened.index = 24  then      'HI_VAL23'
                            when    flattened.index = 25  then      'HI_VAL24'
                            when    flattened.index = 26  then      'HI_VAL25'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^HI.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'HI_PREFIX',
                            'HI_VAL01',
                            'HI_VAL02',
                            'HI_VAL03',
                            'HI_VAL04',
                            'HI_VAL05',
                            'HI_VAL06',
                            'HI_VAL07',
                            'HI_VAL08',
                            'HI_VAL09',
                            'HI_VAL10',
                            'HI_VAL11',
                            'HI_VAL12',
                            'HI_VAL13',
                            'HI_VAL14',
                            'HI_VAL15',
                            'HI_VAL16',
                            'HI_VAL17',
                            'HI_VAL18',
                            'HI_VAL19',
                            'HI_VAL20',
                            'HI_VAL21',
                            'HI_VAL22',
                            'HI_VAL23',
                            'HI_VAL24',
                            'HI_VAL25'
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
                        HI_PREFIX,
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(unpvt.metric_value) as clm_hi_array
    from        pivoted
                unpivot (
                    metric_value for metric_name in (
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
                )   as unpvt
    group by    1,2,3
    order by    1,2,3
)
, clm_ref_flattened as
(
    select      claims.response_id,
                claims.nth_transaction_set,
                claims.claim_index,
                flattened.value['claim_ref_code']           ::varchar as claim_ref_code,
                flattened.value['claim_ref_description']    ::varchar as claim_ref_description,
                flattened.value['claim_ref_value']          ::varchar as claim_ref_value
                
    from        claim_ref as claims,
                lateral flatten(input => clm_ref_array) as flattened
)
, clm_ref_ea as
(
    select      *
    from        clm_ref_flattened
    where       claim_ref_code = 'EA'
    --Ensure uniqueness
    qualify     row_number() over (partition by response_id, nth_transaction_set, claim_index order by claim_ref_value asc) = 1
)
, clm_ref_g1 as
(
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(claim_ref_value) as claim_ref_value_array
    from        clm_ref_flattened
    where       claim_ref_code = 'G1'
    group by    1,2,3
)
select      header.response_id,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.clm_prefix,
            header.claim_id,
            header.total_claim_charge,
            header.patient_control_number,
            header.facility_code_value,
            header.place_of_service,
            header.provider_signature_on_file,
            header.assignment_plan_participation,
            header.benefits_assignment_indicator,
            header.release_of_info_code,
            header.claim_11,
            dtp_435.dtp_prefix_claim_admit,
            dtp_435.date_qualifier_claim_admit,
            dtp_435.date_format_claim_admit,
            dtp_435.datetime_claim_admit,
            dtp_435.admit_date_claim,
            dtp_435.admit_hour_claim,
            dtp_439.dtp_prefix_accident,
            dtp_439.date_qualifier_accident,
            dtp_439.date_format_accident,
            dtp_439.date_range_accident,
            dtp_096.dtp_prefix_discharge,
            dtp_096.date_qualifier_discharge,
            dtp_096.date_format_discharge,
            dtp_096.time_discharge,
            dtp_484.dtp_prefix_billing_period,
            dtp_484.date_qualifier_billing_period,
            dtp_484.date_format_billing_period,
            dtp_484.time_billing_period,
            nm82.name_code_rendering,
            nm82.entity_identifier_code_rendering,
            nm82.entity_type_qualifier_rendering,
            nm82.last_name_org_rendering,
            nm82.first_name_rendering,
            nm82.middle_name_rendering,
            nm82.name_prefix_rendering,
            nm82.name_suffix_rendering,
            nm82.id_code_qualifier_rendering,
            nm82.id_code_rendering,
            nm82_prv.prv_prefix_rendering,
            nm82_prv.provider_code_rendering,
            nm82_prv.reference_id_qualifier_rendering,
            nm82_prv.provider_taxonomy_code_rendering,
            nm77.name_code_service_loc,
            nm77.entity_identifier_code_service_loc,
            nm77.entity_type_qualifier_service_loc,
            nm77.last_name_org_service_loc,
            nm77.first_name_service_loc,
            nm77.middle_name_service_loc,
            nm77.name_prefix_service_loc,
            nm77.name_suffix_service_loc,
            nm77.id_code_qualifier_service_loc,
            nm77.id_code_service_loc,
            nm77_prv.prv_prefix_service_loc,
            nm77_prv.provider_code_service_loc,
            nm77_prv.reference_id_qualifier_service_loc,
            nm77_prv.provider_taxonomy_code_service_loc,
            nmdn.name_code_referring,
            nmdn.entity_identifier_code_referring,
            nmdn.entity_type_qualifier_referring,
            nmdn.last_name_org_referring,
            nmdn.first_name_referring,
            nmdn.middle_name_referring,
            nmdn.name_prefix_referring,
            nmdn.name_suffix_referring,
            nmdn.id_code_qualifier_referring,
            nmdn.id_code_referring,
            nmpw.name_code_prior_payer,
            nmpw.entity_identifier_code_prior_payer,
            nmpw.entity_type_qualifier_prior_payer,
            nmpw.last_name_org_prior_payer,
            nmpw.first_name_prior_payer,
            nmpw.middle_name_prior_payer,
            nmpw.name_prefix_prior_payer,
            nmpw.name_suffix_prior_payer,
            nmpw.id_code_qualifier_prior_payer,
            nmpw.id_code_prior_payer,
            nmpw_n3.address_code_prior_payer_n3,
            nmpw_n3.address_line_1_prior_payer,
            nmpw_n3.address_line_2_prior_payer,
            nmpw_n4.address_code_prior_payer_n4,
            nmpw_n4.city_prior_payer,
            nmpw_n4.st_prior_payer,
            nmpw_n4.zip_prior_payer,
            nmpw_n4.country_prior_payer,
            nmpw_n4.location_qualifier_prior_payer,
            nmpw_n4.location_identifier_prior_payer,
            nm45.name_code_dropoff,
            nm45.entity_identifier_code_dropoff,
            nm45.entity_type_qualifier_dropoff,
            nm45.last_name_org_dropoff,
            nm45.first_name_dropoff,
            nm45.middle_name_dropoff,
            nm45.name_prefix_dropoff,
            nm45.name_suffix_dropoff,
            nm45.id_code_qualifier_dropoff,
            nm45.id_code_dropoff,
            nm45_n3.address_code_dropoff_n3,
            nm45_n3.address_line_1_dropoff,
            nm45_n3.address_line_2_dropoff,
            nm45_n4.address_code_dropoff_n4,
            nm45_n4.city_dropoff,
            nm45_n4.st_dropoff,
            nm45_n4.zip_dropoff,
            nm45_n4.country_dropoff,
            nm45_n4.location_qualifier_dropoff,
            nm45_n4.location_identifier_dropoff,
            nte.clm_nte_prefix,
            nte.clm_note_ref_code,
            nte.clm_note_description,

            ref.clm_ref_array,
            hi.clm_hi_array,
            pwk.clm_pwk_array,

            clm_ref_ea.claim_ref_value          as clm_ref_medical_record_num,
            clm_ref_g1.claim_ref_value_array    as clm_ref_treatment_auth_codes_array

from        header_clm      as header
            left join
                claim_dtp_435 as dtp_435
                on  header.response_id          = dtp_435.response_id
                and header.nth_transaction_set  = dtp_435.nth_transaction_set
                and header.claim_index          = dtp_435.claim_index
            left join
                claim_dtp_439 as dtp_439
                on  header.response_id          = dtp_439.response_id
                and header.nth_transaction_set  = dtp_439.nth_transaction_set
                and header.claim_index          = dtp_439.claim_index
            left join
                claim_dtp_096 as dtp_096
                on  header.response_id          = dtp_096.response_id
                and header.nth_transaction_set  = dtp_096.nth_transaction_set
                and header.claim_index          = dtp_096.claim_index
            left join
                claim_dtp_484 as dtp_484
                on  header.response_id          = dtp_484.response_id
                and header.nth_transaction_set  = dtp_484.nth_transaction_set
                and header.claim_index          = dtp_484.claim_index
            left join
                claim_nm82 as nm82
                on  header.response_id          = nm82.response_id
                and header.nth_transaction_set  = nm82.nth_transaction_set
                and header.claim_index          = nm82.claim_index
            left join
                claim_nm82_prv as nm82_prv
                on  header.response_id          = nm82_prv.response_id
                and header.nth_transaction_set  = nm82_prv.nth_transaction_set
                and header.claim_index          = nm82_prv.claim_index
            left join
                claim_nm77 as nm77
                on  header.response_id          = nm77.response_id
                and header.nth_transaction_set  = nm77.nth_transaction_set
                and header.claim_index          = nm77.claim_index
            left join
                claim_nm77_prv as nm77_prv
                on  header.response_id          = nm77_prv.response_id
                and header.nth_transaction_set  = nm77_prv.nth_transaction_set
                and header.claim_index          = nm77_prv.claim_index
            left join
                claim_nmDN as nmDN
                on  header.response_id          = nmDN.response_id
                and header.nth_transaction_set  = nmDN.nth_transaction_set
                and header.claim_index          = nmDN.claim_index
            left join
                claim_nmPW as nmPW
                on  header.response_id          = nmPW.response_id
                and header.nth_transaction_set  = nmPW.nth_transaction_set
                and header.claim_index          = nmPW.claim_index
            left join
                claim_nmPW_n3 as nmPW_n3
                on  header.response_id          = nmPW_n3.response_id
                and header.nth_transaction_set  = nmPW_n3.nth_transaction_set
                and header.claim_index          = nmPW_n3.claim_index
            left join
                claim_nmPW_n4 as nmPW_n4
                on  header.response_id          = nmPW_n4.response_id
                and header.nth_transaction_set  = nmPW_n4.nth_transaction_set
                and header.claim_index          = nmPW_n4.claim_index
            left join
                claim_nm45 as nm45
                on  header.response_id          = nm45.response_id
                and header.nth_transaction_set  = nm45.nth_transaction_set
                and header.claim_index          = nm45.claim_index
            left join
                claim_nm45_n3 as nm45_n3
                on  header.response_id          = nm45_n3.response_id
                and header.nth_transaction_set  = nm45_n3.nth_transaction_set
                and header.claim_index          = nm45_n3.claim_index
            left join
                claim_nm45_n4 as nm45_n4
                on  header.response_id          = nm45_n4.response_id
                and header.nth_transaction_set  = nm45_n4.nth_transaction_set
                and header.claim_index          = nm45_n4.claim_index
            left join
                claim_pwk as pwk
                on  header.response_id          = pwk.response_id
                and header.nth_transaction_set  = pwk.nth_transaction_set
                and header.claim_index          = pwk.claim_index
            left join
                claim_nte as nte
                on  header.response_id          = nte.response_id
                and header.nth_transaction_set  = nte.nth_transaction_set
                and header.claim_index          = nte.claim_index
            left join
                claim_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
            left join
                claim_hi as hi
                on  header.response_id          = hi.response_id
                and header.nth_transaction_set  = hi.nth_transaction_set
                and header.claim_index          = hi.claim_index
            left join
                clm_ref_ea
                on  header.response_id          = clm_ref_ea.response_id
                and header.nth_transaction_set  = clm_ref_ea.nth_transaction_set
                and header.claim_index          = clm_ref_ea.claim_index
            left join
                clm_ref_g1
                on  header.response_id          = clm_ref_g1.response_id
                and header.nth_transaction_set  = clm_ref_g1.nth_transaction_set
                and header.claim_index          = clm_ref_g1.claim_index        

order by    1,2,3
;



create or replace task
    edwprodhh.edi_837p_parser.insert_claims
    warehouse = analysis_wh
    after edwprodhh.edi_837p_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837p_parser.claims
(
    RESPONSE_ID,
    NTH_TRANSACTION_SET,
    INDEX,
    HL_INDEX_CURRENT,
    HL_INDEX_BILLING_20,
    HL_INDEX_SUBSCRIBER_22,
    HL_INDEX_PATIENT_23,
    CLAIM_INDEX,
    CLM_PREFIX,
    CLAIM_ID,
    TOTAL_CLAIM_CHARGE,
    PATIENT_CONTROL_NUMBER,
    FACILITY_CODE_VALUE,
    PLACE_OF_SERVICE,
    PROVIDER_SIGNATURE_ON_FILE,
    ASSIGNMENT_PLAN_PARTICIPATION,
    BENEFITS_ASSIGNMENT_INDICATOR,
    RELEASE_OF_INFO_CODE,
    CLAIM_11,
    DTP_PREFIX_CLAIM_ADMIT,
    DATE_QUALIFIER_CLAIM_ADMIT,
    DATE_FORMAT_CLAIM_ADMIT,
    DATETIME_CLAIM_ADMIT,
    ADMIT_DATE_CLAIM,
    ADMIT_HOUR_CLAIM,
    DTP_PREFIX_ACCIDENT,
    DATE_QUALIFIER_ACCIDENT,
    DATE_FORMAT_ACCIDENT,
    DATE_RANGE_ACCIDENT,
    DTP_PREFIX_DISCHARGE,
    DATE_QUALIFIER_DISCHARGE,
    DATE_FORMAT_DISCHARGE,
    TIME_DISCHARGE,
    DTP_PREFIX_BILLING_PERIOD,
    DATE_QUALIFIER_BILLING_PERIOD,
    DATE_FORMAT_BILLING_PERIOD,
    TIME_BILLING_PERIOD,
    NAME_CODE_RENDERING,
    ENTITY_IDENTIFIER_CODE_RENDERING,
    ENTITY_TYPE_QUALIFIER_RENDERING,
    LAST_NAME_ORG_RENDERING,
    FIRST_NAME_RENDERING,
    MIDDLE_NAME_RENDERING,
    NAME_PREFIX_RENDERING,
    NAME_SUFFIX_RENDERING,
    ID_CODE_QUALIFIER_RENDERING,
    ID_CODE_RENDERING,
    PRV_PREFIX_RENDERING,
    PROVIDER_CODE_RENDERING,
    REFERENCE_ID_QUALIFIER_RENDERING,
    PROVIDER_TAXONOMY_CODE_RENDERING,
    NAME_CODE_SERVICE_LOC,
    ENTITY_IDENTIFIER_CODE_SERVICE_LOC,
    ENTITY_TYPE_QUALIFIER_SERVICE_LOC,
    LAST_NAME_ORG_SERVICE_LOC,
    FIRST_NAME_SERVICE_LOC,
    MIDDLE_NAME_SERVICE_LOC,
    NAME_PREFIX_SERVICE_LOC,
    NAME_SUFFIX_SERVICE_LOC,
    ID_CODE_QUALIFIER_SERVICE_LOC,
    ID_CODE_SERVICE_LOC,
    PRV_PREFIX_SERVICE_LOC,
    PROVIDER_CODE_SERVICE_LOC,
    REFERENCE_ID_QUALIFIER_SERVICE_LOC,
    PROVIDER_TAXONOMY_CODE_SERVICE_LOC,
    NAME_CODE_REFERRING,
    ENTITY_IDENTIFIER_CODE_REFERRING,
    ENTITY_TYPE_QUALIFIER_REFERRING,
    LAST_NAME_ORG_REFERRING,
    FIRST_NAME_REFERRING,
    MIDDLE_NAME_REFERRING,
    NAME_PREFIX_REFERRING,
    NAME_SUFFIX_REFERRING,
    ID_CODE_QUALIFIER_REFERRING,
    ID_CODE_REFERRING,
    NAME_CODE_PRIOR_PAYER,
    ENTITY_IDENTIFIER_CODE_PRIOR_PAYER,
    ENTITY_TYPE_QUALIFIER_PRIOR_PAYER,
    LAST_NAME_ORG_PRIOR_PAYER,
    FIRST_NAME_PRIOR_PAYER,
    MIDDLE_NAME_PRIOR_PAYER,
    NAME_PREFIX_PRIOR_PAYER,
    NAME_SUFFIX_PRIOR_PAYER,
    ID_CODE_QUALIFIER_PRIOR_PAYER,
    ID_CODE_PRIOR_PAYER,
    ADDRESS_CODE_PRIOR_PAYER_N3,
    ADDRESS_LINE_1_PRIOR_PAYER,
    ADDRESS_LINE_2_PRIOR_PAYER,
    ADDRESS_CODE_PRIOR_PAYER_N4,
    CITY_PRIOR_PAYER,
    ST_PRIOR_PAYER,
    ZIP_PRIOR_PAYER,
    COUNTRY_PRIOR_PAYER,
    LOCATION_QUALIFIER_PRIOR_PAYER,
    LOCATION_IDENTIFIER_PRIOR_PAYER,
    NAME_CODE_DROPOFF,
    ENTITY_IDENTIFIER_CODE_DROPOFF,
    ENTITY_TYPE_QUALIFIER_DROPOFF,
    LAST_NAME_ORG_DROPOFF,
    FIRST_NAME_DROPOFF,
    MIDDLE_NAME_DROPOFF,
    NAME_PREFIX_DROPOFF,
    NAME_SUFFIX_DROPOFF,
    ID_CODE_QUALIFIER_DROPOFF,
    ID_CODE_DROPOFF,
    ADDRESS_CODE_DROPOFF_N3,
    ADDRESS_LINE_1_DROPOFF,
    ADDRESS_LINE_2_DROPOFF,
    ADDRESS_CODE_DROPOFF_N4,
    CITY_DROPOFF,
    ST_DROPOFF,
    ZIP_DROPOFF,
    COUNTRY_DROPOFF,
    LOCATION_QUALIFIER_DROPOFF,
    LOCATION_IDENTIFIER_DROPOFF,
    CLM_NTE_PREFIX,
    CLM_NOTE_REF_CODE,
    CLM_NOTE_DESCRIPTION,
    CLM_REF_ARRAY,
    CLM_HI_ARRAY,
    CLM_PWK_ARRAY,
    CLM_REF_MEDICAL_RECORD_NUM,
    CLM_REF_TREATMENT_AUTH_CODES_ARRAY
)
with filtered_clm as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_837p_parser.claims)
                and claim_index is not null --0 Pre-Filter
                and lx_index is null
)
, header_clm as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_PREFIX'
                            when    flattened.index = 2   then      'CLAIM_ID'
                            when    flattened.index = 3   then      'TOTAL_CLAIM_CHARGE'
                            when    flattened.index = 4   then      'PATIENT_CONTROL_NUMBER'
                            when    flattened.index = 5   then      'FACILITY_CODE_VALUE'
                            when    flattened.index = 6   then      'PLACE_OF_SERVICE'           --11/13/21 Office/Hospital/Inpatient : ...
                            when    flattened.index = 7   then      'PROVIDER_SIGNATURE_ON_FILE'
                            when    flattened.index = 8   then      'ASSIGNMENT_PLAN_PARTICIPATION'
                            when    flattened.index = 9   then      'BENEFITS_ASSIGNMENT_INDICATOR'
                            when    flattened.index = 10  then      'RELEASE_OF_INFO_CODE'
                            when    flattened.index = 11  then      'CLAIM_11'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^CLM.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_PREFIX',
                        'CLAIM_ID',
                        'TOTAL_CLAIM_CHARGE',
                        'PATIENT_CONTROL_NUMBER',
                        'FACILITY_CODE_VALUE',
                        'PLACE_OF_SERVICE',
                        'PROVIDER_SIGNATURE_ON_FILE',
                        'ASSIGNMENT_PLAN_PARTICIPATION',
                        'BENEFITS_ASSIGNMENT_INDICATOR',
                        'RELEASE_OF_INFO_CODE',
                        'CLAIM_11'
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
                    CLM_PREFIX,
                    CLAIM_ID,
                    TOTAL_CLAIM_CHARGE,
                    PATIENT_CONTROL_NUMBER,
                    FACILITY_CODE_VALUE,
                    PLACE_OF_SERVICE,
                    PROVIDER_SIGNATURE_ON_FILE,
                    ASSIGNMENT_PLAN_PARTICIPATION,
                    BENEFITS_ASSIGNMENT_INDICATOR,
                    RELEASE_OF_INFO_CODE,
                    CLAIM_11
                )
)
, claim_dtp_435 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM_ADMIT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM_ADMIT'
                            when    flattened.index = 3   then      'DATE_FORMAT_CLAIM_ADMIT'
                            when    flattened.index = 4   then      'DATETIME_CLAIM_ADMIT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*435.*')                          --1 Filter
    )
    select      *,
                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    to_date(left(datetime_claim_admit,  8), 'YYYYMMDD')
                        else    NULL
                        end     as admit_date_claim,

                case    when    date_format_claim_admit = 'DT'
                        and     regexp_like(datetime_claim_admit, '^\\d{12}$')
                        then    left(right(datetime_claim_admit, 4), 2)
                        else    NULL
                        end     as admit_hour_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_CLAIM_ADMIT',
                        'DATE_QUALIFIER_CLAIM_ADMIT',
                        'DATE_FORMAT_CLAIM_ADMIT',
                        'DATETIME_CLAIM_ADMIT'
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
                    DTP_PREFIX_CLAIM_ADMIT,
                    DATE_QUALIFIER_CLAIM_ADMIT,
                    DATE_FORMAT_CLAIM_ADMIT,
                    DATETIME_CLAIM_ADMIT
                )
)
, claim_dtp_439 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_ACCIDENT'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_ACCIDENT'
                            when    flattened.index = 3   then      'DATE_FORMAT_ACCIDENT'
                            when    flattened.index = 4   then      'DATE_RANGE_ACCIDENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*434.*')                          --1 Filter
    )
    select      *
                -- case    when    date_format_claim = 'RD8'
                --         and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                --         then    to_date(left(date_range_claim,  8), 'YYYYMMDD')
                --         else    NULL
                --         end     as start_date_claim,

                -- case    when    date_format_claim = 'RD8'
                --         and     regexp_like(date_range_claim, '^\\d{8}\\-\\d{8}$')
                --         then    to_date(right(date_range_claim, 8), 'YYYYMMDD')
                --         else    NULL
                --         end     as end_date_claim
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_ACCIDENT',
                        'DATE_QUALIFIER_ACCIDENT',
                        'DATE_FORMAT_ACCIDENT',
                        'DATE_RANGE_ACCIDENT'
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
                    DTP_PREFIX_ACCIDENT,
                    DATE_QUALIFIER_ACCIDENT,
                    DATE_FORMAT_ACCIDENT,
                    DATE_RANGE_ACCIDENT
                )
)
, claim_dtp_096 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_DISCHARGE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_DISCHARGE'
                            when    flattened.index = 3   then      'DATE_FORMAT_DISCHARGE'
                            when    flattened.index = 4   then      'TIME_DISCHARGE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*096.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_DISCHARGE',
                        'DATE_QUALIFIER_DISCHARGE',
                        'DATE_FORMAT_DISCHARGE',
                        'TIME_DISCHARGE'
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
                    DTP_PREFIX_DISCHARGE,
                    DATE_QUALIFIER_DISCHARGE,
                    DATE_FORMAT_DISCHARGE,
                    TIME_DISCHARGE
                )
)
, claim_dtp_484 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_BILLING_PERIOD'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_BILLING_PERIOD'
                            when    flattened.index = 3   then      'DATE_FORMAT_BILLING_PERIOD'
                            when    flattened.index = 4   then      'TIME_BILLING_PERIOD'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened            --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^DTP\\*484.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_BILLING_PERIOD',
                        'DATE_QUALIFIER_BILLING_PERIOD',
                        'DATE_FORMAT_BILLING_PERIOD',
                        'TIME_BILLING_PERIOD'
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
                    DTP_PREFIX_BILLING_PERIOD,
                    DATE_QUALIFIER_BILLING_PERIOD,
                    DATE_FORMAT_BILLING_PERIOD,
                    TIME_BILLING_PERIOD
                )
)
, claim_nm82 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_RENDERING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_RENDERING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_RENDERING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_RENDERING'
                            when    flattened.index = 5   then      'FIRST_NAME_RENDERING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_RENDERING'
                            when    flattened.index = 7   then      'NAME_PREFIX_RENDERING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_RENDERING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_RENDERING'
                            when    flattened.index = 10  then      'ID_CODE_RENDERING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*82.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_RENDERING',
                        'ENTITY_IDENTIFIER_CODE_RENDERING',
                        'ENTITY_TYPE_QUALIFIER_RENDERING',
                        'LAST_NAME_ORG_RENDERING',
                        'FIRST_NAME_RENDERING',
                        'MIDDLE_NAME_RENDERING',
                        'NAME_PREFIX_RENDERING',
                        'NAME_SUFFIX_RENDERING',
                        'ID_CODE_QUALIFIER_RENDERING',
                        'ID_CODE_RENDERING'
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
                    NAME_CODE_RENDERING,
                    ENTITY_IDENTIFIER_CODE_RENDERING,
                    ENTITY_TYPE_QUALIFIER_RENDERING,
                    LAST_NAME_ORG_RENDERING,
                    FIRST_NAME_RENDERING,
                    MIDDLE_NAME_RENDERING,
                    NAME_PREFIX_RENDERING,
                    NAME_SUFFIX_RENDERING,
                    ID_CODE_QUALIFIER_RENDERING,
                    ID_CODE_RENDERING
                )
)
, claim_nm82_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_RENDERING'
                            when    flattened.index = 2   then      'PROVIDER_CODE_RENDERING'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_RENDERING'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_RENDERING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*82'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_RENDERING',
                        'PROVIDER_CODE_RENDERING',
                        'REFERENCE_ID_QUALIFIER_RENDERING',
                        'PROVIDER_TAXONOMY_CODE_RENDERING'
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
                    PRV_PREFIX_RENDERING,
                    PROVIDER_CODE_RENDERING,
                    REFERENCE_ID_QUALIFIER_RENDERING,
                    PROVIDER_TAXONOMY_CODE_RENDERING
                )
)
, claim_nm77 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_SERVICE_LOC'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SERVICE_LOC'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_SERVICE_LOC'
                            when    flattened.index = 5   then      'FIRST_NAME_SERVICE_LOC'
                            when    flattened.index = 6   then      'MIDDLE_NAME_SERVICE_LOC'
                            when    flattened.index = 7   then      'NAME_PREFIX_SERVICE_LOC'
                            when    flattened.index = 8   then      'NAME_SUFFIX_SERVICE_LOC'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 10  then      'ID_CODE_SERVICE_LOC'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*77.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_SERVICE_LOC',
                        'ENTITY_IDENTIFIER_CODE_SERVICE_LOC',
                        'ENTITY_TYPE_QUALIFIER_SERVICE_LOC',
                        'LAST_NAME_ORG_SERVICE_LOC',
                        'FIRST_NAME_SERVICE_LOC',
                        'MIDDLE_NAME_SERVICE_LOC',
                        'NAME_PREFIX_SERVICE_LOC',
                        'NAME_SUFFIX_SERVICE_LOC',
                        'ID_CODE_QUALIFIER_SERVICE_LOC',
                        'ID_CODE_SERVICE_LOC'
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
                    NAME_CODE_SERVICE_LOC,
                    ENTITY_IDENTIFIER_CODE_SERVICE_LOC,
                    ENTITY_TYPE_QUALIFIER_SERVICE_LOC,
                    LAST_NAME_ORG_SERVICE_LOC,
                    FIRST_NAME_SERVICE_LOC,
                    MIDDLE_NAME_SERVICE_LOC,
                    NAME_PREFIX_SERVICE_LOC,
                    NAME_SUFFIX_SERVICE_LOC,
                    ID_CODE_QUALIFIER_SERVICE_LOC,
                    ID_CODE_SERVICE_LOC
                )
)
, claim_nm77_prv as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_SERVICE_LOC'
                            when    flattened.index = 2   then      'PROVIDER_CODE_SERVICE_LOC'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_SERVICE_LOC'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_SERVICE_LOC'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*77'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_SERVICE_LOC',
                        'PROVIDER_CODE_SERVICE_LOC',
                        'REFERENCE_ID_QUALIFIER_SERVICE_LOC',
                        'PROVIDER_TAXONOMY_CODE_SERVICE_LOC'
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
                    PRV_PREFIX_SERVICE_LOC,
                    PROVIDER_CODE_SERVICE_LOC,
                    REFERENCE_ID_QUALIFIER_SERVICE_LOC,
                    PROVIDER_TAXONOMY_CODE_SERVICE_LOC
                )
)
, claim_nmDN as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_REFERRING'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_REFERRING'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_REFERRING'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_REFERRING'
                            when    flattened.index = 5   then      'FIRST_NAME_REFERRING'
                            when    flattened.index = 6   then      'MIDDLE_NAME_REFERRING'
                            when    flattened.index = 7   then      'NAME_PREFIX_REFERRING'
                            when    flattened.index = 8   then      'NAME_SUFFIX_REFERRING'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_REFERRING'
                            when    flattened.index = 10  then      'ID_CODE_REFERRING'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*DN.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_REFERRING',
                        'ENTITY_IDENTIFIER_CODE_REFERRING',
                        'ENTITY_TYPE_QUALIFIER_REFERRING',
                        'LAST_NAME_ORG_REFERRING',
                        'FIRST_NAME_REFERRING',
                        'MIDDLE_NAME_REFERRING',
                        'NAME_PREFIX_REFERRING',
                        'NAME_SUFFIX_REFERRING',
                        'ID_CODE_QUALIFIER_REFERRING',
                        'ID_CODE_REFERRING'
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
                    NAME_CODE_REFERRING,
                    ENTITY_IDENTIFIER_CODE_REFERRING,
                    ENTITY_TYPE_QUALIFIER_REFERRING,
                    LAST_NAME_ORG_REFERRING,
                    FIRST_NAME_REFERRING,
                    MIDDLE_NAME_REFERRING,
                    NAME_PREFIX_REFERRING,
                    NAME_SUFFIX_REFERRING,
                    ID_CODE_QUALIFIER_REFERRING,
                    ID_CODE_REFERRING
                )
)
, claim_nmPW as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_PRIOR_PAYER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_PRIOR_PAYER'
                            when    flattened.index = 5   then      'FIRST_NAME_PRIOR_PAYER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_PRIOR_PAYER'
                            when    flattened.index = 7   then      'NAME_PREFIX_PRIOR_PAYER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_PRIOR_PAYER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 10  then      'ID_CODE_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*PW.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_PRIOR_PAYER',
                        'ENTITY_IDENTIFIER_CODE_PRIOR_PAYER',
                        'ENTITY_TYPE_QUALIFIER_PRIOR_PAYER',
                        'LAST_NAME_ORG_PRIOR_PAYER',
                        'FIRST_NAME_PRIOR_PAYER',
                        'MIDDLE_NAME_PRIOR_PAYER',
                        'NAME_PREFIX_PRIOR_PAYER',
                        'NAME_SUFFIX_PRIOR_PAYER',
                        'ID_CODE_QUALIFIER_PRIOR_PAYER',
                        'ID_CODE_PRIOR_PAYER'
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
                    NAME_CODE_PRIOR_PAYER,
                    ENTITY_IDENTIFIER_CODE_PRIOR_PAYER,
                    ENTITY_TYPE_QUALIFIER_PRIOR_PAYER,
                    LAST_NAME_ORG_PRIOR_PAYER,
                    FIRST_NAME_PRIOR_PAYER,
                    MIDDLE_NAME_PRIOR_PAYER,
                    NAME_PREFIX_PRIOR_PAYER,
                    NAME_SUFFIX_PRIOR_PAYER,
                    ID_CODE_QUALIFIER_PRIOR_PAYER,
                    ID_CODE_PRIOR_PAYER
                )
)
, claim_nmPW_n3 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PRIOR_PAYER_N3'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N3.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*PW'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PRIOR_PAYER_N3',
                        'ADDRESS_LINE_1_PRIOR_PAYER',
                        'ADDRESS_LINE_2_PRIOR_PAYER'
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
                    ADDRESS_CODE_PRIOR_PAYER_N3,
                    ADDRESS_LINE_1_PRIOR_PAYER,
                    ADDRESS_LINE_2_PRIOR_PAYER
                )
)
, claim_nmPW_n4 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_PRIOR_PAYER_N4'
                            when    flattened.index = 2   then      'CITY_PRIOR_PAYER'
                            when    flattened.index = 3   then      'ST_PRIOR_PAYER'
                            when    flattened.index = 4   then      'ZIP_PRIOR_PAYER'
                            when    flattened.index = 5   then      'COUNTRY_PRIOR_PAYER'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_PRIOR_PAYER'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PRIOR_PAYER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N4.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*PW'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_PRIOR_PAYER_N4',
                        'CITY_PRIOR_PAYER',
                        'ST_PRIOR_PAYER',
                        'ZIP_PRIOR_PAYER',
                        'COUNTRY_PRIOR_PAYER',
                        'LOCATION_QUALIFIER_PRIOR_PAYER',
                        'LOCATION_IDENTIFIER_PRIOR_PAYER'
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
                    ADDRESS_CODE_PRIOR_PAYER_N4,
                    CITY_PRIOR_PAYER,
                    ST_PRIOR_PAYER,
                    ZIP_PRIOR_PAYER,
                    COUNTRY_PRIOR_PAYER,
                    LOCATION_QUALIFIER_PRIOR_PAYER,
                    LOCATION_IDENTIFIER_PRIOR_PAYER
                )
)
, claim_nm45 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_DROPOFF'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_DROPOFF'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_DROPOFF'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_DROPOFF'
                            when    flattened.index = 5   then      'FIRST_NAME_DROPOFF'
                            when    flattened.index = 6   then      'MIDDLE_NAME_DROPOFF'
                            when    flattened.index = 7   then      'NAME_PREFIX_DROPOFF'
                            when    flattened.index = 8   then      'NAME_SUFFIX_DROPOFF'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_DROPOFF'
                            when    flattened.index = 10  then      'ID_CODE_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NM1\\*45.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_DROPOFF',
                        'ENTITY_IDENTIFIER_CODE_DROPOFF',
                        'ENTITY_TYPE_QUALIFIER_DROPOFF',
                        'LAST_NAME_ORG_DROPOFF',
                        'FIRST_NAME_DROPOFF',
                        'MIDDLE_NAME_DROPOFF',
                        'NAME_PREFIX_DROPOFF',
                        'NAME_SUFFIX_DROPOFF',
                        'ID_CODE_QUALIFIER_DROPOFF',
                        'ID_CODE_DROPOFF'
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
                    NAME_CODE_DROPOFF,
                    ENTITY_IDENTIFIER_CODE_DROPOFF,
                    ENTITY_TYPE_QUALIFIER_DROPOFF,
                    LAST_NAME_ORG_DROPOFF,
                    FIRST_NAME_DROPOFF,
                    MIDDLE_NAME_DROPOFF,
                    NAME_PREFIX_DROPOFF,
                    NAME_SUFFIX_DROPOFF,
                    ID_CODE_QUALIFIER_DROPOFF,
                    ID_CODE_DROPOFF
                )
)
, claim_nm45_n3 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_DROPOFF_N3'
                            when    flattened.index = 2   then      'ADDRESS_LINE_1_DROPOFF'
                            when    flattened.index = 3   then      'ADDRESS_LINE_2_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N3.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*45'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_DROPOFF_N3',
                        'ADDRESS_LINE_1_DROPOFF',
                        'ADDRESS_LINE_2_DROPOFF'
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
                    ADDRESS_CODE_DROPOFF_N3,
                    ADDRESS_LINE_1_DROPOFF,
                    ADDRESS_LINE_2_DROPOFF
                )
)
, claim_nm45_n4 as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'ADDRESS_CODE_DROPOFF_N4'
                            when    flattened.index = 2   then      'CITY_DROPOFF'
                            when    flattened.index = 3   then      'ST_DROPOFF'
                            when    flattened.index = 4   then      'ZIP_DROPOFF'
                            when    flattened.index = 5   then      'COUNTRY_DROPOFF'
                            when    flattened.index = 6   then      'LOCATION_QUALIFIER_DROPOFF'
                            when    flattened.index = 7   then      'LOCATION_IDENTIFIER_DROPOFF'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^N4.*')                         --1 Filter
                    and filtered_clm.lag_name_indicator = 'NM1*45'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'ADDRESS_CODE_DROPOFF_N4',
                        'CITY_DROPOFF',
                        'ST_DROPOFF',
                        'ZIP_DROPOFF',
                        'COUNTRY_DROPOFF',
                        'LOCATION_QUALIFIER_DROPOFF',
                        'LOCATION_IDENTIFIER_DROPOFF'
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
                    ADDRESS_CODE_DROPOFF_N4,
                    CITY_DROPOFF,
                    ST_DROPOFF,
                    ZIP_DROPOFF,
                    COUNTRY_DROPOFF,
                    LOCATION_QUALIFIER_DROPOFF,
                    LOCATION_IDENTIFIER_DROPOFF
                )
)
, claim_pwk as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PWK_PREFIX'
                            when    flattened.index = 2   then      'PWK_REPORT_TYPE_CODE'
                            when    flattened.index = 3   then      'PWK_TRANSMISSION_CODE'
                            when    flattened.index = 4   then      'PWK_COPIES_NEEDED'
                            when    flattened.index = 5   then      'PWK_ENTITY_IDENTIFIER'
                            when    flattened.index = 6   then      'PWK_ID_QUALIFIER'
                            when    flattened.index = 7   then      'PWK_CONTROL_NUMBER_ATTACHMENT'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^PWK.*')                          --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'PWK_PREFIX',
                            'PWK_REPORT_TYPE_CODE',
                            'PWK_TRANSMISSION_CODE',
                            'PWK_COPIES_NEEDED',
                            'PWK_ENTITY_IDENTIFIER',
                            'PWK_ID_QUALIFIER',
                            'PWK_CONTROL_NUMBER_ATTACHMENT'
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
                        PWK_PREFIX,
                        PWK_REPORT_TYPE_CODE,
                        PWK_TRANSMISSION_CODE,
                        PWK_COPIES_NEEDED,
                        PWK_ENTITY_IDENTIFIER,
                        PWK_ID_QUALIFIER,
                        PWK_CONTROL_NUMBER_ATTACHMENT
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'pwk_prefix',                       pwk_prefix::varchar,
                        'pwk_report_type_code',             pwk_report_type_code::varchar,
                        'pwk_transmission_code',            pwk_transmission_code::varchar,
                        'pwk_copies_needed',                pwk_copies_needed::varchar,
                        'pwk_entity_identifier',            pwk_entity_identifier::varchar,
                        'pwk_id_qualifier',                 pwk_id_qualifier::varchar,
                        'pwk_control_number_attachment',    pwk_control_number_attachment::varchar
                    )
                )   as clm_pwk_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, claim_nte as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CLM_NTE_PREFIX'
                            when    flattened.index = 2   then      'CLM_NOTE_REF_CODE'
                            when    flattened.index = 3   then      'CLM_NOTE_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^NTE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CLM_NTE_PREFIX',
                        'CLM_NOTE_REF_CODE',
                        'CLM_NOTE_DESCRIPTION'
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
                    CLM_NTE_PREFIX,
                    CLM_NOTE_REF_CODE,
                    CLM_NOTE_DESCRIPTION
                )
)
, claim_ref as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_CLAIM'
                            when    flattened.index = 2   then      'REFERENCE_ID_CODE_CLAIM'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'REFERENCE_ID_CLAIM'
                            when    flattened.index = 4   then      'DESCRIPTION_CLAIM'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^REF.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_CLAIM',
                            'REFERENCE_ID_CODE_CLAIM',
                            'REFERENCE_ID_CLAIM',
                            'DESCRIPTION_CLAIM'
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
                        REF_PREFIX_CLAIM,
                        REFERENCE_ID_CODE_CLAIM,
                        REFERENCE_ID_CLAIM,
                        DESCRIPTION_CLAIM
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_code_claim::varchar,
                        'claim_ref_value',          reference_id_claim::varchar,
                        'claim_ref_description',    description_claim::varchar
                    )
                )   as clm_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, claim_hi as
(
    with long as
    (
        select      filtered_clm.response_id,
                    filtered_clm.nth_transaction_set,
                    filtered_clm.index,
                    filtered_clm.hl_index_current,
                    filtered_clm.hl_index_billing_20,
                    filtered_clm.hl_index_subscriber_22,
                    filtered_clm.hl_index_patient_23,
                    filtered_clm.claim_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'HI_PREFIX'
                            when    flattened.index = 2   then      'HI_VAL01'
                            when    flattened.index = 3   then      'HI_VAL02'
                            when    flattened.index = 4   then      'HI_VAL03'
                            when    flattened.index = 5   then      'HI_VAL04'
                            when    flattened.index = 6   then      'HI_VAL05'
                            when    flattened.index = 7   then      'HI_VAL06'
                            when    flattened.index = 8   then      'HI_VAL07'
                            when    flattened.index = 9   then      'HI_VAL08'
                            when    flattened.index = 10  then      'HI_VAL09'
                            when    flattened.index = 11  then      'HI_VAL10'
                            when    flattened.index = 12  then      'HI_VAL11'
                            when    flattened.index = 13  then      'HI_VAL12'
                            when    flattened.index = 14  then      'HI_VAL13'
                            when    flattened.index = 15  then      'HI_VAL14'
                            when    flattened.index = 16  then      'HI_VAL15'
                            when    flattened.index = 17  then      'HI_VAL16'
                            when    flattened.index = 18  then      'HI_VAL17'
                            when    flattened.index = 19  then      'HI_VAL18'
                            when    flattened.index = 20 then       'HI_VAL19'
                            when    flattened.index = 21  then      'HI_VAL20'
                            when    flattened.index = 22  then      'HI_VAL21'
                            when    flattened.index = 23  then      'HI_VAL22'
                            when    flattened.index = 24  then      'HI_VAL23'
                            when    flattened.index = 25  then      'HI_VAL24'
                            when    flattened.index = 26  then      'HI_VAL25'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_clm,
                    lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

        where       regexp_like(filtered_clm.line_element_837, '^HI.*')                            --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'HI_PREFIX',
                            'HI_VAL01',
                            'HI_VAL02',
                            'HI_VAL03',
                            'HI_VAL04',
                            'HI_VAL05',
                            'HI_VAL06',
                            'HI_VAL07',
                            'HI_VAL08',
                            'HI_VAL09',
                            'HI_VAL10',
                            'HI_VAL11',
                            'HI_VAL12',
                            'HI_VAL13',
                            'HI_VAL14',
                            'HI_VAL15',
                            'HI_VAL16',
                            'HI_VAL17',
                            'HI_VAL18',
                            'HI_VAL19',
                            'HI_VAL20',
                            'HI_VAL21',
                            'HI_VAL22',
                            'HI_VAL23',
                            'HI_VAL24',
                            'HI_VAL25'
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
                        HI_PREFIX,
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(unpvt.metric_value) as clm_hi_array
    from        pivoted
                unpivot (
                    metric_value for metric_name in (
                        HI_VAL01,
                        HI_VAL02,
                        HI_VAL03,
                        HI_VAL04,
                        HI_VAL05,
                        HI_VAL06,
                        HI_VAL07,
                        HI_VAL08,
                        HI_VAL09,
                        HI_VAL10,
                        HI_VAL11,
                        HI_VAL12,
                        HI_VAL13,
                        HI_VAL14,
                        HI_VAL15,
                        HI_VAL16,
                        HI_VAL17,
                        HI_VAL18,
                        HI_VAL19,
                        HI_VAL20,
                        HI_VAL21,
                        HI_VAL22,
                        HI_VAL23,
                        HI_VAL24,
                        HI_VAL25
                    )
                )   as unpvt
    group by    1,2,3
    order by    1,2,3
)
, clm_ref_flattened as
(
    select      claims.response_id,
                claims.nth_transaction_set,
                claims.claim_index,
                flattened.value['claim_ref_code']           ::varchar as claim_ref_code,
                flattened.value['claim_ref_description']    ::varchar as claim_ref_description,
                flattened.value['claim_ref_value']          ::varchar as claim_ref_value
                
    from        claim_ref as claims,
                lateral flatten(input => clm_ref_array) as flattened
)
, clm_ref_ea as
(
    select      *
    from        clm_ref_flattened
    where       claim_ref_code = 'EA'
    --Ensure uniqueness
    qualify     row_number() over (partition by response_id, nth_transaction_set, claim_index order by claim_ref_value asc) = 1
)
, clm_ref_g1 as
(
    select      response_id,
                nth_transaction_set,
                claim_index,
                array_agg(claim_ref_value) as claim_ref_value_array
    from        clm_ref_flattened
    where       claim_ref_code = 'G1'
    group by    1,2,3
)
select      header.response_id,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.clm_prefix,
            header.claim_id,
            header.total_claim_charge,
            header.patient_control_number,
            header.facility_code_value,
            header.place_of_service,
            header.provider_signature_on_file,
            header.assignment_plan_participation,
            header.benefits_assignment_indicator,
            header.release_of_info_code,
            header.claim_11,
            dtp_435.dtp_prefix_claim_admit,
            dtp_435.date_qualifier_claim_admit,
            dtp_435.date_format_claim_admit,
            dtp_435.datetime_claim_admit,
            dtp_435.admit_date_claim,
            dtp_435.admit_hour_claim,
            dtp_439.dtp_prefix_accident,
            dtp_439.date_qualifier_accident,
            dtp_439.date_format_accident,
            dtp_439.date_range_accident,
            dtp_096.dtp_prefix_discharge,
            dtp_096.date_qualifier_discharge,
            dtp_096.date_format_discharge,
            dtp_096.time_discharge,
            dtp_484.dtp_prefix_billing_period,
            dtp_484.date_qualifier_billing_period,
            dtp_484.date_format_billing_period,
            dtp_484.time_billing_period,
            nm82.name_code_rendering,
            nm82.entity_identifier_code_rendering,
            nm82.entity_type_qualifier_rendering,
            nm82.last_name_org_rendering,
            nm82.first_name_rendering,
            nm82.middle_name_rendering,
            nm82.name_prefix_rendering,
            nm82.name_suffix_rendering,
            nm82.id_code_qualifier_rendering,
            nm82.id_code_rendering,
            nm82_prv.prv_prefix_rendering,
            nm82_prv.provider_code_rendering,
            nm82_prv.reference_id_qualifier_rendering,
            nm82_prv.provider_taxonomy_code_rendering,
            nm77.name_code_service_loc,
            nm77.entity_identifier_code_service_loc,
            nm77.entity_type_qualifier_service_loc,
            nm77.last_name_org_service_loc,
            nm77.first_name_service_loc,
            nm77.middle_name_service_loc,
            nm77.name_prefix_service_loc,
            nm77.name_suffix_service_loc,
            nm77.id_code_qualifier_service_loc,
            nm77.id_code_service_loc,
            nm77_prv.prv_prefix_service_loc,
            nm77_prv.provider_code_service_loc,
            nm77_prv.reference_id_qualifier_service_loc,
            nm77_prv.provider_taxonomy_code_service_loc,
            nmdn.name_code_referring,
            nmdn.entity_identifier_code_referring,
            nmdn.entity_type_qualifier_referring,
            nmdn.last_name_org_referring,
            nmdn.first_name_referring,
            nmdn.middle_name_referring,
            nmdn.name_prefix_referring,
            nmdn.name_suffix_referring,
            nmdn.id_code_qualifier_referring,
            nmdn.id_code_referring,
            nmpw.name_code_prior_payer,
            nmpw.entity_identifier_code_prior_payer,
            nmpw.entity_type_qualifier_prior_payer,
            nmpw.last_name_org_prior_payer,
            nmpw.first_name_prior_payer,
            nmpw.middle_name_prior_payer,
            nmpw.name_prefix_prior_payer,
            nmpw.name_suffix_prior_payer,
            nmpw.id_code_qualifier_prior_payer,
            nmpw.id_code_prior_payer,
            nmpw_n3.address_code_prior_payer_n3,
            nmpw_n3.address_line_1_prior_payer,
            nmpw_n3.address_line_2_prior_payer,
            nmpw_n4.address_code_prior_payer_n4,
            nmpw_n4.city_prior_payer,
            nmpw_n4.st_prior_payer,
            nmpw_n4.zip_prior_payer,
            nmpw_n4.country_prior_payer,
            nmpw_n4.location_qualifier_prior_payer,
            nmpw_n4.location_identifier_prior_payer,
            nm45.name_code_dropoff,
            nm45.entity_identifier_code_dropoff,
            nm45.entity_type_qualifier_dropoff,
            nm45.last_name_org_dropoff,
            nm45.first_name_dropoff,
            nm45.middle_name_dropoff,
            nm45.name_prefix_dropoff,
            nm45.name_suffix_dropoff,
            nm45.id_code_qualifier_dropoff,
            nm45.id_code_dropoff,
            nm45_n3.address_code_dropoff_n3,
            nm45_n3.address_line_1_dropoff,
            nm45_n3.address_line_2_dropoff,
            nm45_n4.address_code_dropoff_n4,
            nm45_n4.city_dropoff,
            nm45_n4.st_dropoff,
            nm45_n4.zip_dropoff,
            nm45_n4.country_dropoff,
            nm45_n4.location_qualifier_dropoff,
            nm45_n4.location_identifier_dropoff,
            nte.clm_nte_prefix,
            nte.clm_note_ref_code,
            nte.clm_note_description,

            ref.clm_ref_array,
            hi.clm_hi_array,
            pwk.clm_pwk_array,

            clm_ref_ea.claim_ref_value          as clm_ref_medical_record_num,
            clm_ref_g1.claim_ref_value_array    as clm_ref_treatment_auth_codes_array

from        header_clm      as header
            left join
                claim_dtp_435 as dtp_435
                on  header.response_id          = dtp_435.response_id
                and header.nth_transaction_set  = dtp_435.nth_transaction_set
                and header.claim_index          = dtp_435.claim_index
            left join
                claim_dtp_439 as dtp_439
                on  header.response_id          = dtp_439.response_id
                and header.nth_transaction_set  = dtp_439.nth_transaction_set
                and header.claim_index          = dtp_439.claim_index
            left join
                claim_dtp_096 as dtp_096
                on  header.response_id          = dtp_096.response_id
                and header.nth_transaction_set  = dtp_096.nth_transaction_set
                and header.claim_index          = dtp_096.claim_index
            left join
                claim_dtp_484 as dtp_484
                on  header.response_id          = dtp_484.response_id
                and header.nth_transaction_set  = dtp_484.nth_transaction_set
                and header.claim_index          = dtp_484.claim_index
            left join
                claim_nm82 as nm82
                on  header.response_id          = nm82.response_id
                and header.nth_transaction_set  = nm82.nth_transaction_set
                and header.claim_index          = nm82.claim_index
            left join
                claim_nm82_prv as nm82_prv
                on  header.response_id          = nm82_prv.response_id
                and header.nth_transaction_set  = nm82_prv.nth_transaction_set
                and header.claim_index          = nm82_prv.claim_index
            left join
                claim_nm77 as nm77
                on  header.response_id          = nm77.response_id
                and header.nth_transaction_set  = nm77.nth_transaction_set
                and header.claim_index          = nm77.claim_index
            left join
                claim_nm77_prv as nm77_prv
                on  header.response_id          = nm77_prv.response_id
                and header.nth_transaction_set  = nm77_prv.nth_transaction_set
                and header.claim_index          = nm77_prv.claim_index
            left join
                claim_nmDN as nmDN
                on  header.response_id          = nmDN.response_id
                and header.nth_transaction_set  = nmDN.nth_transaction_set
                and header.claim_index          = nmDN.claim_index
            left join
                claim_nmPW as nmPW
                on  header.response_id          = nmPW.response_id
                and header.nth_transaction_set  = nmPW.nth_transaction_set
                and header.claim_index          = nmPW.claim_index
            left join
                claim_nmPW_n3 as nmPW_n3
                on  header.response_id          = nmPW_n3.response_id
                and header.nth_transaction_set  = nmPW_n3.nth_transaction_set
                and header.claim_index          = nmPW_n3.claim_index
            left join
                claim_nmPW_n4 as nmPW_n4
                on  header.response_id          = nmPW_n4.response_id
                and header.nth_transaction_set  = nmPW_n4.nth_transaction_set
                and header.claim_index          = nmPW_n4.claim_index
            left join
                claim_nm45 as nm45
                on  header.response_id          = nm45.response_id
                and header.nth_transaction_set  = nm45.nth_transaction_set
                and header.claim_index          = nm45.claim_index
            left join
                claim_nm45_n3 as nm45_n3
                on  header.response_id          = nm45_n3.response_id
                and header.nth_transaction_set  = nm45_n3.nth_transaction_set
                and header.claim_index          = nm45_n3.claim_index
            left join
                claim_nm45_n4 as nm45_n4
                on  header.response_id          = nm45_n4.response_id
                and header.nth_transaction_set  = nm45_n4.nth_transaction_set
                and header.claim_index          = nm45_n4.claim_index
            left join
                claim_pwk as pwk
                on  header.response_id          = pwk.response_id
                and header.nth_transaction_set  = pwk.nth_transaction_set
                and header.claim_index          = pwk.claim_index
            left join
                claim_nte as nte
                on  header.response_id          = nte.response_id
                and header.nth_transaction_set  = nte.nth_transaction_set
                and header.claim_index          = nte.claim_index
            left join
                claim_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
            left join
                claim_hi as hi
                on  header.response_id          = hi.response_id
                and header.nth_transaction_set  = hi.nth_transaction_set
                and header.claim_index          = hi.claim_index
            left join
                clm_ref_ea
                on  header.response_id          = clm_ref_ea.response_id
                and header.nth_transaction_set  = clm_ref_ea.nth_transaction_set
                and header.claim_index          = clm_ref_ea.claim_index
            left join
                clm_ref_g1
                on  header.response_id          = clm_ref_g1.response_id
                and header.nth_transaction_set  = clm_ref_g1.nth_transaction_set
                and header.claim_index          = clm_ref_g1.claim_index        

order by    1,2,3
;