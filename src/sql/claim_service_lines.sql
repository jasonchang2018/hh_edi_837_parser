create or replace table
    edwprodhh.edi_837_parser.claim_service_lines
as
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837_parser.response_flat
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
, servline_lx_sv2 as
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

                    case    when    flattened.index = 1   then      'SV2_PREFIX'
                            when    flattened.index = 2   then      'REVENUE_CODE'
                            when    flattened.index = 3   then      'PROCEDURE_CODE'
                            when    flattened.index = 4   then      'CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'MEASUREMENT_CODE'
                            when    flattened.index = 6   then      'SERVICE_UNITS'
                            when    flattened.index = 7   then      'SV2_MOD_1'
                            when    flattened.index = 8   then      'SV2_MOD_2'
                            when    flattened.index = 9   then      'SV2_MOD_3'
                            when    flattened.index = 10  then      'SV2_MOD_4'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV2.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV2_PREFIX',
                        'REVENUE_CODE',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'SV2_MOD_1',
                        'SV2_MOD_2',
                        'SV2_MOD_3',
                        'SV2_MOD_4'
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
                    SV2_PREFIX,
                    REVENUE_CODE,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    SV2_MOD_1,
                    SV2_MOD_2,
                    SV2_MOD_3,
                    SV2_MOD_4
                )
)
, servline_lx_dtp as
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

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX'
                            when    flattened.index = 4   then      'DATE_LX'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX',
                        'DATE_QUALIFIER_LX',
                        'DATE_FORMAT_LX',
                        'DATE_LX'
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
                    DTP_PREFIX_LX,
                    DATE_QUALIFIER_LX,
                    DATE_FORMAT_LX,
                    DATE_LX
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
            sv2.sv2_prefix,
            sv2.revenue_code,
            sv2.procedure_code,
            sv2.charge_amount,
            sv2.measurement_code,
            sv2.service_units,
            sv2.sv2_mod_1,
            sv2.sv2_mod_2,
            sv2.sv2_mod_3,
            sv2.sv2_mod_4,
            dtp.dtp_prefix_lx,
            dtp.date_qualifier_lx,
            dtp.date_format_lx,
            dtp.date_lx,
            ref.ref_prefix_lx,
            ref.reference_id_qualifier_lx,
            ref.reference_id_lx,
            ref.description_lx

from        servline_lx_header as header
            left join
                servline_lx_sv2 as sv2
                on  header.response_id               = sv2.response_id
                and header.nth_transaction_set  = sv2.nth_transaction_set
                and header.claim_index          = sv2.claim_index
                and header.lx_index             = sv2.lx_index
            left join
                servline_lx_dtp as dtp
                on  header.response_id               = dtp.response_id
                and header.nth_transaction_set  = dtp.nth_transaction_set
                and header.claim_index          = dtp.claim_index
                and header.lx_index             = dtp.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id               = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;



insert into
    edwprodhh.edi_837_parser.claim_service_lines
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
    SV2_PREFIX,
    REVENUE_CODE,
    PROCEDURE_CODE,
    CHARGE_AMOUNT,
    MEASUREMENT_CODE,
    SERVICE_UNITS,
    SV2_MOD_1,
    SV2_MOD_2,
    SV2_MOD_3,
    SV2_MOD_4,
    DTP_PREFIX_LX,
    DATE_QUALIFIER_LX,
    DATE_FORMAT_LX,
    DATE_LX,
    REF_PREFIX_LX,
    REFERENCE_ID_QUALIFIER_LX,
    REFERENCE_ID_LX,
    DESCRIPTION_LX
)
with filtered_lx as
(
    select      *
    from        edwprodhh.edi_837_parser.response_flat
    where       claim_index is not null --0 Pre-Filter
                and lx_index is not null
                and response_id not in (select response_id from edwprodhh.edi_837_parser.claim_service_lines)
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
, servline_lx_sv2 as
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

                    case    when    flattened.index = 1   then      'SV2_PREFIX'
                            when    flattened.index = 2   then      'REVENUE_CODE'
                            when    flattened.index = 3   then      'PROCEDURE_CODE'
                            when    flattened.index = 4   then      'CHARGE_AMOUNT'
                            when    flattened.index = 5   then      'MEASUREMENT_CODE'
                            when    flattened.index = 6   then      'SERVICE_UNITS'
                            when    flattened.index = 7   then      'SV2_MOD_1'
                            when    flattened.index = 8   then      'SV2_MOD_2'
                            when    flattened.index = 9   then      'SV2_MOD_3'
                            when    flattened.index = 10  then      'SV2_MOD_4'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^SV2.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'SV2_PREFIX',
                        'REVENUE_CODE',
                        'PROCEDURE_CODE',
                        'CHARGE_AMOUNT',
                        'MEASUREMENT_CODE',
                        'SERVICE_UNITS',
                        'SV2_MOD_1',
                        'SV2_MOD_2',
                        'SV2_MOD_3',
                        'SV2_MOD_4'
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
                    SV2_PREFIX,
                    REVENUE_CODE,
                    PROCEDURE_CODE,
                    CHARGE_AMOUNT,
                    MEASUREMENT_CODE,
                    SERVICE_UNITS,
                    SV2_MOD_1,
                    SV2_MOD_2,
                    SV2_MOD_3,
                    SV2_MOD_4
                )
)
, servline_lx_dtp as
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

                    case    when    flattened.index = 1   then      'DTP_PREFIX_LX'
                            when    flattened.index = 2   then      'DATE_QUALIFIER_LX'
                            when    flattened.index = 3   then      'DATE_FORMAT_LX'
                            when    flattened.index = 4   then      'DATE_LX'
                            end     as value_header,

                    case    when    value_header = 'DATE_LX'
                            and     regexp_like(flattened.value, '^\\d{8}$')
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered_lx,
                    lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(filtered_lx.line_element_837, '^DTP.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTP_PREFIX_LX',
                        'DATE_QUALIFIER_LX',
                        'DATE_FORMAT_LX',
                        'DATE_LX'
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
                    DTP_PREFIX_LX,
                    DATE_QUALIFIER_LX,
                    DATE_FORMAT_LX,
                    DATE_LX
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
            sv2.sv2_prefix,
            sv2.revenue_code,
            sv2.procedure_code,
            sv2.charge_amount,
            sv2.measurement_code,
            sv2.service_units,
            sv2.sv2_mod_1,
            sv2.sv2_mod_2,
            sv2.sv2_mod_3,
            sv2.sv2_mod_4,
            dtp.dtp_prefix_lx,
            dtp.date_qualifier_lx,
            dtp.date_format_lx,
            dtp.date_lx,
            ref.ref_prefix_lx,
            ref.reference_id_qualifier_lx,
            ref.reference_id_lx,
            ref.description_lx

from        servline_lx_header as header
            left join
                servline_lx_sv2 as sv2
                on  header.response_id               = sv2.response_id
                and header.nth_transaction_set  = sv2.nth_transaction_set
                and header.claim_index          = sv2.claim_index
                and header.lx_index             = sv2.lx_index
            left join
                servline_lx_dtp as dtp
                on  header.response_id               = dtp.response_id
                and header.nth_transaction_set  = dtp.nth_transaction_set
                and header.claim_index          = dtp.claim_index
                and header.lx_index             = dtp.lx_index
            left join
                servline_lx_ref as ref
                on  header.response_id               = ref.response_id
                and header.nth_transaction_set  = ref.nth_transaction_set
                and header.claim_index          = ref.claim_index
                and header.lx_index             = ref.lx_index
                
order by    1,2,3
;