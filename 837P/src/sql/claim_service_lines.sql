create or replace table
    edwprodhh.edi_837p_parser.claim_service_lines
as
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is not null
)
, servline_lx_header as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_ASSIGNED_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LX.*')                         --1 Filter
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
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, servline_lx_sv1 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SV1_PREFIX'
                            when    flattened.index = 2   then      'PROCEDURE_CODE'
                            when    flattened.index = 3   then      'CHARGE_AMOUNT'
                            when    flattened.index = 4   then      'MEASUREMENT_CODE'
                            when    flattened.index = 5   then      'SERVICE_UNITS'
                            when    flattened.index = 6   then      'PLACE_OF_SERVICE_CODE'
                            when    flattened.index = 7   then      'EMERGENCY_INDICATOR'
                            when    flattened.index = 8   then      'DIAGNOSIS_CODE_POINTERS'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV1.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV1_PREFIX',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'PLACE_OF_SERVICE_CODE',
                        'EMERGENCY_INDICATOR',
                        'DIAGNOSIS_CODE_POINTERS'
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
                    SV1_PREFIX,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    PLACE_OF_SERVICE_CODE,
                    EMERGENCY_INDICATOR,
                    DIAGNOSIS_CODE_POINTERS
                )
)
, servline_lx_dtp_471 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*471.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION',
                        'DATE_QUALIFIER_LX_PRESCRIPTION',
                        'DATE_FORMAT_LX_PRESCRIPTION',
                        'DATE_LX_PRESCRIPTION'
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
                    DTP_PREFIX_LX_PRESCRIPTION,
                    DATE_QUALIFIER_LX_PRESCRIPTION,
                    DATE_FORMAT_LX_PRESCRIPTION,
                    DATE_LX_PRESCRIPTION
                )
)
, servline_lx_dtp_472 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_SERVICE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_SERVICE'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_SERVICE'
                            when    flattened.index = 4   then      'DATE_LX_SERVICE'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_SERVICE'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*472.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_SERVICE',
                        'DATE_QUALIFIER_LX_SERVICE',
                        'DATE_FORMAT_LX_SERVICE',
                        'DATE_LX_SERVICE'
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
                    DTP_PREFIX_LX_SERVICE,
                    DATE_QUALIFIER_LX_SERVICE,
                    DATE_FORMAT_LX_SERVICE,
                    DATE_LX_SERVICE
                )
)
, servline_lx_nm82 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_REND_PROVIDER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_REND_PROVIDER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_REND_PROVIDER'
                            when    flattened.index = 5   then      'FIRST_NAME_REND_PROVIDER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_REND_PROVIDER'
                            when    flattened.index = 7   then      'NAME_PREFIX_REND_PROVIDER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_REND_PROVIDER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 10  then      'ID_CODE_REND_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^NM1\\*82.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_REND_PROVIDER',
                        'ENTITY_IDENTIFIER_CODE_REND_PROVIDER',
                        'ENTITY_TYPE_QUALIFIER_REND_PROVIDER',
                        'LAST_NAME_ORG_REND_PROVIDER',
                        'FIRST_NAME_REND_PROVIDER',
                        'MIDDLE_NAME_REND_PROVIDER',
                        'NAME_PREFIX_REND_PROVIDER',
                        'NAME_SUFFIX_REND_PROVIDER',
                        'ID_CODE_QUALIFIER_REND_PROVIDER',
                        'ID_CODE_REND_PROVIDER'
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
                    NAME_CODE_REND_PROVIDER,
                    ENTITY_IDENTIFIER_CODE_REND_PROVIDER,
                    ENTITY_TYPE_QUALIFIER_REND_PROVIDER,
                    LAST_NAME_ORG_REND_PROVIDER,
                    FIRST_NAME_REND_PROVIDER,
                    MIDDLE_NAME_REND_PROVIDER,
                    NAME_PREFIX_REND_PROVIDER,
                    NAME_SUFFIX_REND_PROVIDER,
                    ID_CODE_QUALIFIER_REND_PROVIDER,
                    ID_CODE_REND_PROVIDER
                )
)
, servline_lx_nm82_prv as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_REND_PROVIDER'
                            when    flattened.index = 2   then      'PROVIDER_CODE_REND_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_REND_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^PRV.*')                          --1 Filter
                    and filtered_lx.lag_name_indicator = 'NM1*82'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_REND_PROVIDER',
                        'PROVIDER_CODE_REND_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_REND_PROVIDER',
                        'PROVIDER_TAXONOMY_CODE_REND_PROVIDER'
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
                    PRV_PREFIX_REND_PROVIDER,
                    PROVIDER_CODE_REND_PROVIDER,
                    REFERENCE_ID_QUALIFIER_REND_PROVIDER,
                    PROVIDER_TAXONOMY_CODE_REND_PROVIDER
                )
)
, servline_lx_nte as --array? could cause duplicates
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_NTE_PREFIX'
                            when    flattened.index = 2   then      'LX_NOTE_REF_CODE'
                            when    flattened.index = 3   then      'LX_NOTE_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^NTE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_NTE_PREFIX',
                        'LX_NOTE_REF_CODE',
                        'LX_NOTE_DESCRIPTION'
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
                    LX_NTE_PREFIX,
                    LX_NOTE_REF_CODE,
                    LX_NOTE_DESCRIPTION
                )
)
, servline_lx_lin as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LIN_PREFIX'
                            when    flattened.index = 2   then      'LIN_ASSIGNED_ID'
                            when    flattened.index = 3   then      'LIN_PRODUCT_QUALIFIER'
                            when    flattened.index = 4   then      'LIN_PRODUCT_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LIN.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LIN_PREFIX',
                        'LIN_ASSIGNED_ID',
                        'LIN_PRODUCT_QUALIFIER',
                        'LIN_PRODUCT_ID'
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
                    LIN_PREFIX,
                    LIN_ASSIGNED_ID,
                    LIN_PRODUCT_QUALIFIER,
                    LIN_PRODUCT_ID
                )
)
, servline_lx_ctp as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CTP_PREFIX'
                            when    flattened.index = 2   then      'CTP_CLASS'
                            when    flattened.index = 3   then      'CTP_PRICE_CODE'
                            when    flattened.index = 4   then      'CTP_UNIT_PRICE'
                            when    flattened.index = 5   then      'CTP_UNIT_MEASURE'
                            when    flattened.index = 6   then      'CTP_QUANTITY'
                            when    flattened.index = 7   then      'CTP_7'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^CTP.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CTP_PREFIX',
                        'CTP_CLASS',
                        'CTP_PRICE_CODE',
                        'CTP_UNIT_PRICE',
                        'CTP_UNIT_MEASURE',
                        'CTP_QUANTITY',
                        'CTP_7'
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
                    CTP_PREFIX,
                    CTP_CLASS,
                    CTP_PRICE_CODE,
                    CTP_UNIT_PRICE,
                    CTP_UNIT_MEASURE,
                    CTP_QUANTITY,
                    CTP_7
                )
)
, servline_lx_ref as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_LX'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_LX'
                            when    flattened.index = 3   then      'REFERENCE_ID_LX'
                            when    flattened.index = 4   then      'DESCRIPTION_LX'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_LX',
                            'REFERENCE_ID_QUALIFIER_LX',
                            'REFERENCE_ID_LX',
                            'DESCRIPTION_LX'
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
                        REF_PREFIX_LX,
                        REFERENCE_ID_QUALIFIER_LX,
                        REFERENCE_ID_LX,
                        DESCRIPTION_LX
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_qualifier_lx::varchar,
                        'claim_ref_value',          reference_id_lx::varchar,
                        'claim_ref_description',    description_lx::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
select      header.response_id,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.lx_index,
            header.lx_prefix,
            header.lx_assigned_line_number,
            sv1.sv1_prefix,
            sv1.procedure_code,
            sv1.charge_amount,
            sv1.measurement_code,
            sv1.service_units,
            sv1.place_of_service_code,
            sv1.emergency_indicator,
            sv1.diagnosis_code_pointers,
            dtp_471.dtp_prefix_lx_prescription,
            dtp_471.date_qualifier_lx_prescription,
            dtp_471.date_format_lx_prescription,
            dtp_471.date_lx_prescription,
            dtp_472.dtp_prefix_lx_service,
            dtp_472.date_qualifier_lx_service,
            dtp_472.date_format_lx_service,
            dtp_472.date_lx_service,
            nm82.name_code_rend_provider,
            nm82.entity_identifier_code_rend_provider,
            nm82.entity_type_qualifier_rend_provider,
            nm82.last_name_org_rend_provider,
            nm82.first_name_rend_provider,
            nm82.middle_name_rend_provider,
            nm82.name_prefix_rend_provider,
            nm82.name_suffix_rend_provider,
            nm82.id_code_qualifier_rend_provider,
            nm82.id_code_rend_provider,
            nm82_prv.prv_prefix_rend_provider,
            nm82_prv.provider_code_rend_provider,
            nm82_prv.reference_id_qualifier_rend_provider,
            nm82_prv.provider_taxonomy_code_rend_provider,
            nte.lx_nte_prefix,
            nte.lx_note_ref_code,
            nte.lx_note_description,
            lin.lin_prefix,
            lin.lin_assigned_id,
            lin.lin_product_qualifier,
            lin.lin_product_id,
            ctp.ctp_prefix,
            ctp.ctp_class,
            ctp.ctp_price_code,
            ctp.ctp_unit_price,
            ctp.ctp_unit_measure,
            ctp.ctp_quantity,
            ctp.ctp_7,
            ref.lx_ref_array

from        servline_lx_header as header
            left join
                servline_lx_sv1 as sv1
                on  header.response_id          = sv1.response_id
                and header.nth_transaction_set  = sv1.nth_transaction_set
                and header.claim_index          = sv1.claim_index
                and header.lx_index             = sv1.lx_index
            left join
                servline_lx_dtp_471 as dtp_471
                on  header.response_id          = dtp_471.response_id
                and header.nth_transaction_set  = dtp_471.nth_transaction_set
                and header.claim_index          = dtp_471.claim_index
                and header.lx_index             = dtp_471.lx_index
            left join
                servline_lx_dtp_472 as dtp_472
                on  header.response_id          = dtp_472.response_id
                and header.nth_transaction_set  = dtp_472.nth_transaction_set
                and header.claim_index          = dtp_472.claim_index
                and header.lx_index             = dtp_472.lx_index
            left join
                servline_lx_nm82 as nm82
                on  header.response_id          = nm82.response_id
                and header.nth_transaction_set  = nm82.nth_transaction_set
                and header.claim_index          = nm82.claim_index
                and header.lx_index             = nm82.lx_index
            left join
                servline_lx_nm82_prv as nm82_prv
                on  header.response_id          = nm82_prv.response_id
                and header.nth_transaction_set  = nm82_prv.nth_transaction_set
                and header.claim_index          = nm82_prv.claim_index
                and header.lx_index             = nm82_prv.lx_index
            left join
                servline_lx_nte as nte
                on  header.response_id          = nte.response_id
                and header.nth_transaction_set  = nte.nth_transaction_set
                and header.claim_index          = nte.claim_index
                and header.lx_index             = nte.lx_index
            left join
                servline_lx_lin as lin
                on  header.response_id          = lin.response_id
                and header.nth_transaction_set  = lin.nth_transaction_set
                and header.claim_index          = lin.claim_index
                and header.lx_index             = lin.lx_index
            left join
                servline_lx_ctp as ctp
                on  header.response_id          = ctp.response_id
                and header.nth_transaction_set  = ctp.nth_transaction_set
                and header.claim_index          = ctp.claim_index
                and header.lx_index             = ctp.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;



create or replace task
    edwprodhh.edi_837p_parser.insert_claim_service_lines
    warehouse = analysis_wh
    after edwprodhh.edi_837p_parser.insert_response_flat
as
insert into
    edwprodhh.edi_837p_parser.claim_service_lines
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
    LX_PREFIX,
    LX_ASSIGNED_LINE_NUMBER,
    SV1_PREFIX,
    PROCEDURE_CODE,
    CHARGE_AMOUNT,
    MEASUREMENT_CODE,
    SERVICE_UNITS,
    PLACE_OF_SERVICE_CODE,
    EMERGENCY_INDICATOR,
    DIAGNOSIS_CODE_POINTERS,
    DTP_PREFIX_LX_PRESCRIPTION,
    DATE_QUALIFIER_LX_PRESCRIPTION,
    DATE_FORMAT_LX_PRESCRIPTION,
    DATE_LX_PRESCRIPTION,
    DTP_PREFIX_LX_SERVICE,
    DATE_QUALIFIER_LX_SERVICE,
    DATE_FORMAT_LX_SERVICE,
    DATE_LX_SERVICE,
    NAME_CODE_REND_PROVIDER,
    ENTITY_IDENTIFIER_CODE_REND_PROVIDER,
    ENTITY_TYPE_QUALIFIER_REND_PROVIDER,
    LAST_NAME_ORG_REND_PROVIDER,
    FIRST_NAME_REND_PROVIDER,
    MIDDLE_NAME_REND_PROVIDER,
    NAME_PREFIX_REND_PROVIDER,
    NAME_SUFFIX_REND_PROVIDER,
    ID_CODE_QUALIFIER_REND_PROVIDER,
    ID_CODE_REND_PROVIDER,
    PRV_PREFIX_REND_PROVIDER,
    PROVIDER_CODE_REND_PROVIDER,
    REFERENCE_ID_QUALIFIER_REND_PROVIDER,
    PROVIDER_TAXONOMY_CODE_REND_PROVIDER,
    NTE_PREFIX,
    NOTE_REF_CODE,
    NOTE_DESCRIPTION,
    LIN_PREFIX,
    LIN_ASSIGNED_ID,
    LIN_PRODUCT_QUALIFIER,
    LIN_PRODUCT_ID,
    CTP_PREFIX,
    CTP_CLASS,
    CTP_PRICE_CODE,
    CTP_UNIT_PRICE,
    CTP_UNIT_MEASURE,
    CTP_QUANTITY,
    CTP_7,
    LX_REF_ARRAY
)
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837p_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_837p_parser.claim_service_lines)
                and claim_index is not null --0 Pre-Filter
                and lx_index is not null
)
, servline_lx_header as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_PREFIX'
                            when    flattened.index = 2   then      'LX_ASSIGNED_LINE_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LX.*')                         --1 Filter
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
                    NTH_TRANSACTION_SET,
                    INDEX,
                    HL_INDEX_CURRENT,
                    HL_INDEX_BILLING_20,
                    HL_INDEX_SUBSCRIBER_22,
                    HL_INDEX_PATIENT_23,
                    CLAIM_INDEX,
                    LX_INDEX,
                    LX_PREFIX,
                    LX_ASSIGNED_LINE_NUMBER
                )
)
, servline_lx_sv1 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'SV1_PREFIX'
                            when    flattened.index = 2   then      'PROCEDURE_CODE'
                            when    flattened.index = 3   then      'CHARGE_AMOUNT'
                            when    flattened.index = 4   then      'MEASUREMENT_CODE'
                            when    flattened.index = 5   then      'SERVICE_UNITS'
                            when    flattened.index = 6   then      'PLACE_OF_SERVICE_CODE'
                            when    flattened.index = 7   then      'EMERGENCY_INDICATOR'
                            when    flattened.index = 8   then      'DIAGNOSIS_CODE_POINTERS'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV1.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV1_PREFIX',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'PLACE_OF_SERVICE_CODE',
                        'EMERGENCY_INDICATOR',
                        'DIAGNOSIS_CODE_POINTERS'
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
                    SV1_PREFIX,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    PLACE_OF_SERVICE_CODE,
                    EMERGENCY_INDICATOR,
                    DIAGNOSIS_CODE_POINTERS
                )
)
, servline_lx_dtp_471 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_PRESCRIPTION'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_PRESCRIPTION'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_PRESCRIPTION'
                            when    flattened.index = 4   then      'DATE_LX_PRESCRIPTION'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_PRESCRIPTION'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*471.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_PRESCRIPTION',
                        'DATE_QUALIFIER_LX_PRESCRIPTION',
                        'DATE_FORMAT_LX_PRESCRIPTION',
                        'DATE_LX_PRESCRIPTION'
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
                    DTP_PREFIX_LX_PRESCRIPTION,
                    DATE_QUALIFIER_LX_PRESCRIPTION,
                    DATE_FORMAT_LX_PRESCRIPTION,
                    DATE_LX_PRESCRIPTION
                )
)
, servline_lx_dtp_472 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX_SERVICE'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX_SERVICE'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX_SERVICE'
                            when    flattened.index = 4   then      'DATE_LX_SERVICE'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX_SERVICE'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP\\*472.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX_SERVICE',
                        'DATE_QUALIFIER_LX_SERVICE',
                        'DATE_FORMAT_LX_SERVICE',
                        'DATE_LX_SERVICE'
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
                    DTP_PREFIX_LX_SERVICE,
                    DATE_QUALIFIER_LX_SERVICE,
                    DATE_FORMAT_LX_SERVICE,
                    DATE_LX_SERVICE
                )
)
, servline_lx_nm82 as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NAME_CODE_REND_PROVIDER'
                            when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_REND_PROVIDER'
                            when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 4   then      'LAST_NAME_ORG_REND_PROVIDER'
                            when    flattened.index = 5   then      'FIRST_NAME_REND_PROVIDER'
                            when    flattened.index = 6   then      'MIDDLE_NAME_REND_PROVIDER'
                            when    flattened.index = 7   then      'NAME_PREFIX_REND_PROVIDER'
                            when    flattened.index = 8   then      'NAME_SUFFIX_REND_PROVIDER'
                            when    flattened.index = 9   then      'ID_CODE_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 10  then      'ID_CODE_REND_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^NM1\\*82.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NAME_CODE_REND_PROVIDER',
                        'ENTITY_IDENTIFIER_CODE_REND_PROVIDER',
                        'ENTITY_TYPE_QUALIFIER_REND_PROVIDER',
                        'LAST_NAME_ORG_REND_PROVIDER',
                        'FIRST_NAME_REND_PROVIDER',
                        'MIDDLE_NAME_REND_PROVIDER',
                        'NAME_PREFIX_REND_PROVIDER',
                        'NAME_SUFFIX_REND_PROVIDER',
                        'ID_CODE_QUALIFIER_REND_PROVIDER',
                        'ID_CODE_REND_PROVIDER'
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
                    NAME_CODE_REND_PROVIDER,
                    ENTITY_IDENTIFIER_CODE_REND_PROVIDER,
                    ENTITY_TYPE_QUALIFIER_REND_PROVIDER,
                    LAST_NAME_ORG_REND_PROVIDER,
                    FIRST_NAME_REND_PROVIDER,
                    MIDDLE_NAME_REND_PROVIDER,
                    NAME_PREFIX_REND_PROVIDER,
                    NAME_SUFFIX_REND_PROVIDER,
                    ID_CODE_QUALIFIER_REND_PROVIDER,
                    ID_CODE_REND_PROVIDER
                )
)
, servline_lx_nm82_prv as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'PRV_PREFIX_REND_PROVIDER'
                            when    flattened.index = 2   then      'PROVIDER_CODE_REND_PROVIDER'
                            when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_REND_PROVIDER'
                            when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_REND_PROVIDER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^PRV.*')                          --1 Filter
                    and filtered_lx.lag_name_indicator = 'NM1*82'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'PRV_PREFIX_REND_PROVIDER',
                        'PROVIDER_CODE_REND_PROVIDER',
                        'REFERENCE_ID_QUALIFIER_REND_PROVIDER',
                        'PROVIDER_TAXONOMY_CODE_REND_PROVIDER'
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
                    PRV_PREFIX_REND_PROVIDER,
                    PROVIDER_CODE_REND_PROVIDER,
                    REFERENCE_ID_QUALIFIER_REND_PROVIDER,
                    PROVIDER_TAXONOMY_CODE_REND_PROVIDER
                )
)
, servline_lx_nte as --array? could cause duplicates
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LX_NTE_PREFIX'
                            when    flattened.index = 2   then      'LX_NOTE_REF_CODE'
                            when    flattened.index = 3   then      'LX_NOTE_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^NTE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LX_NTE_PREFIX',
                        'LX_NOTE_REF_CODE',
                        'LX_NOTE_DESCRIPTION'
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
                    LX_NTE_PREFIX,
                    LX_NOTE_REF_CODE,
                    LX_NOTE_DESCRIPTION
                )
)
, servline_lx_lin as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'LIN_PREFIX'
                            when    flattened.index = 2   then      'LIN_ASSIGNED_ID'
                            when    flattened.index = 3   then      'LIN_PRODUCT_QUALIFIER'
                            when    flattened.index = 4   then      'LIN_PRODUCT_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^LIN.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'LIN_PREFIX',
                        'LIN_ASSIGNED_ID',
                        'LIN_PRODUCT_QUALIFIER',
                        'LIN_PRODUCT_ID'
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
                    LIN_PREFIX,
                    LIN_ASSIGNED_ID,
                    LIN_PRODUCT_QUALIFIER,
                    LIN_PRODUCT_ID
                )
)
, servline_lx_ctp as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'CTP_PREFIX'
                            when    flattened.index = 2   then      'CTP_CLASS'
                            when    flattened.index = 3   then      'CTP_PRICE_CODE'
                            when    flattened.index = 4   then      'CTP_UNIT_PRICE'
                            when    flattened.index = 5   then      'CTP_UNIT_MEASURE'
                            when    flattened.index = 6   then      'CTP_QUANTITY'
                            when    flattened.index = 7   then      'CTP_7'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^CTP.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'CTP_PREFIX',
                        'CTP_CLASS',
                        'CTP_PRICE_CODE',
                        'CTP_UNIT_PRICE',
                        'CTP_UNIT_MEASURE',
                        'CTP_QUANTITY',
                        'CTP_7'
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
                    CTP_PREFIX,
                    CTP_CLASS,
                    CTP_PRICE_CODE,
                    CTP_UNIT_PRICE,
                    CTP_UNIT_MEASURE,
                    CTP_QUANTITY,
                    CTP_7
                )
)
, servline_lx_ref as
(
    with long as
    (
        select      filtered_lx.response_id,
                    filtered_lx.nth_transaction_set,
                    filtered_lx.index,
                    filtered_lx.hl_index_current,
                    filtered_lx.hl_index_billing_20,
                    filtered_lx.hl_index_subscriber_22,
                    filtered_lx.hl_index_patient_23,
                    filtered_lx.claim_index,
                    filtered_lx.lx_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'REF_PREFIX_LX'
                            when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_LX'
                            when    flattened.index = 3   then      'REFERENCE_ID_LX'
                            when    flattened.index = 4   then      'DESCRIPTION_LX'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^REF.*')                         --1 Filter
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'REF_PREFIX_LX',
                            'REFERENCE_ID_QUALIFIER_LX',
                            'REFERENCE_ID_LX',
                            'DESCRIPTION_LX'
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
                        REF_PREFIX_LX,
                        REFERENCE_ID_QUALIFIER_LX,
                        REFERENCE_ID_LX,
                        DESCRIPTION_LX
                    )
    )
    select      response_id,
                nth_transaction_set,
                claim_index,
                lx_index,
                array_agg(
                    object_construct_keep_null(
                        'claim_ref_code',           reference_id_qualifier_lx::varchar,
                        'claim_ref_value',          reference_id_lx::varchar,
                        'claim_ref_description',    description_lx::varchar
                    )
                )   as lx_ref_array
    from        pivoted
    group by    1,2,3,4
    order by    1,2,3,4
)
select      header.response_id,
            header.nth_transaction_set,
            header.index,
            header.hl_index_current,
            header.hl_index_billing_20,
            header.hl_index_subscriber_22,
            header.hl_index_patient_23,
            header.claim_index,
            header.lx_index,
            header.lx_prefix,
            header.lx_assigned_line_number,
            sv1.sv1_prefix,
            sv1.procedure_code,
            sv1.charge_amount,
            sv1.measurement_code,
            sv1.service_units,
            sv1.place_of_service_code,
            sv1.emergency_indicator,
            sv1.diagnosis_code_pointers,
            dtp_471.dtp_prefix_lx_prescription,
            dtp_471.date_qualifier_lx_prescription,
            dtp_471.date_format_lx_prescription,
            dtp_471.date_lx_prescription,
            dtp_472.dtp_prefix_lx_service,
            dtp_472.date_qualifier_lx_service,
            dtp_472.date_format_lx_service,
            dtp_472.date_lx_service,
            nm82.name_code_rend_provider,
            nm82.entity_identifier_code_rend_provider,
            nm82.entity_type_qualifier_rend_provider,
            nm82.last_name_org_rend_provider,
            nm82.first_name_rend_provider,
            nm82.middle_name_rend_provider,
            nm82.name_prefix_rend_provider,
            nm82.name_suffix_rend_provider,
            nm82.id_code_qualifier_rend_provider,
            nm82.id_code_rend_provider,
            nm82_prv.prv_prefix_rend_provider,
            nm82_prv.provider_code_rend_provider,
            nm82_prv.reference_id_qualifier_rend_provider,
            nm82_prv.provider_taxonomy_code_rend_provider,
            nte.lx_nte_prefix,
            nte.lx_note_ref_code,
            nte.lx_note_description,
            lin.lin_prefix,
            lin.lin_assigned_id,
            lin.lin_product_qualifier,
            lin.lin_product_id,
            ctp.ctp_prefix,
            ctp.ctp_class,
            ctp.ctp_price_code,
            ctp.ctp_unit_price,
            ctp.ctp_unit_measure,
            ctp.ctp_quantity,
            ctp.ctp_7,
            ref.lx_ref_array

from        servline_lx_header as header
            left join
                servline_lx_sv1 as sv1
                on  header.response_id          = sv1.response_id
                and header.nth_transaction_set  = sv1.nth_transaction_set
                and header.claim_index          = sv1.claim_index
                and header.lx_index             = sv1.lx_index
            left join
                servline_lx_dtp_471 as dtp_471
                on  header.response_id          = dtp_471.response_id
                and header.nth_transaction_set  = dtp_471.nth_transaction_set
                and header.claim_index          = dtp_471.claim_index
                and header.lx_index             = dtp_471.lx_index
            left join
                servline_lx_dtp_472 as dtp_472
                on  header.response_id          = dtp_472.response_id
                and header.nth_transaction_set  = dtp_472.nth_transaction_set
                and header.claim_index          = dtp_472.claim_index
                and header.lx_index             = dtp_472.lx_index
            left join
                servline_lx_nm82 as nm82
                on  header.response_id          = nm82.response_id
                and header.nth_transaction_set  = nm82.nth_transaction_set
                and header.claim_index          = nm82.claim_index
                and header.lx_index             = nm82.lx_index
            left join
                servline_lx_nm82_prv as nm82_prv
                on  header.response_id          = nm82_prv.response_id
                and header.nth_transaction_set  = nm82_prv.nth_transaction_set
                and header.claim_index          = nm82_prv.claim_index
                and header.lx_index             = nm82_prv.lx_index
            left join
                servline_lx_nte as nte
                on  header.response_id          = nte.response_id
                and header.nth_transaction_set  = nte.nth_transaction_set
                and header.claim_index          = nte.claim_index
                and header.lx_index             = nte.lx_index
            left join
                servline_lx_lin as lin
                on  header.response_id          = lin.response_id
                and header.nth_transaction_set  = lin.nth_transaction_set
                and header.claim_index          = lin.claim_index
                and header.lx_index             = lin.lx_index
            left join
                servline_lx_ctp as ctp
                on  header.response_id          = ctp.response_id
                and header.nth_transaction_set  = ctp.nth_transaction_set
                and header.claim_index          = ctp.claim_index
                and header.lx_index             = ctp.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id          = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;