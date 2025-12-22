create or replace table
    edwprodhh.edi_835_parser.transaction_sets
as
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
)
, header_st as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_HEADER'
                            when    flattened.index = 2   then      'TRANSACTION_SET_ID_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_HEADER'
                            when    flattened.index = 4   then      'IMPLEMENTATION_CONVENTION_REFERENCE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^ST.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_HEADER',
                        'TRANSACTION_SET_ID_CODE',
                        'TRANSACTION_SET_CONTROL_NUMBER_HEADER',
                        'IMPLEMENTATION_CONVENTION_REFERENCE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_HEADER,
                    TRANSACTION_SET_ID_CODE,
                    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
                    IMPLEMENTATION_CONVENTION_REFERENCE
                )
)
, trailer_se as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_TRAILER'
                            when    flattened.index = 2   then      'TRANSACTION_SEGMENT_COUNT'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^SE.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_TRAILER',
                        'TRANSACTION_SEGMENT_COUNT',
                        'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_TRAILER,
                    TRANSACTION_SEGMENT_COUNT,
                    TRANSACTION_SET_CONTROL_NUMBER_TRAILER
                )
)
, bpr as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BPR_HEADER'
                            when    flattened.index = 2   then      'TRANS_HANDLING_CODE'
                            when    flattened.index = 3   then      'TRANS_AMOUNT'
                            when    flattened.index = 4   then      'CREDIT_DEBIT_FLAG'
                            when    flattened.index = 5   then      'PAYMENT_METHOD_CODE'
                            when    flattened.index = 6   then      'PAYMENT_FORMAT_CODE'
                            when    flattened.index = 7   then      'DFI_ID_QUALIFIER_SENDER'
                            when    flattened.index = 8   then      'DFI_ID_SENDER'
                            when    flattened.index = 9   then      'ACCOUNT_NUMBER_QUALIFIER_SENDER'
                            when    flattened.index = 10  then      'ACCOUNT_NUMBER_SENDER'
                            when    flattened.index = 11  then      'BPR_ORIGINATING_COMPANY_ID'
                            when    flattened.index = 12  then      'ORIGINATING_COMPANY_SUPPLEMENTAL_CODE'
                            when    flattened.index = 13  then      'DFI_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 14  then      'DFI_ID_RECEIVER'
                            when    flattened.index = 15  then      'ACCOUNT_NUMBER_QUALIFIER_RECEIVER'
                            when    flattened.index = 16  then      'ACCOUNT_NUMBER_RECEIVER'
                            when    flattened.index = 17  then      'PAYMENT_EFFECTIVE_DATE'
                            end     as value_header,

                    case    when    value_header = 'PAYMENT_EFFECTIVE_DATE'     then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^BPR.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BPR_HEADER',
                        'TRANS_HANDLING_CODE',
                        'TRANS_AMOUNT',
                        'CREDIT_DEBIT_FLAG',
                        'PAYMENT_METHOD_CODE',
                        'PAYMENT_FORMAT_CODE',
                        'DFI_ID_QUALIFIER_SENDER',
                        'DFI_ID_SENDER',
                        'ACCOUNT_NUMBER_QUALIFIER_SENDER',
                        'ACCOUNT_NUMBER_SENDER',
                        'BPR_ORIGINATING_COMPANY_ID',
                        'ORIGINATING_COMPANY_SUPPLEMENTAL_CODE',
                        'DFI_ID_QUALIFIER_RECEIVER',
                        'DFI_ID_RECEIVER',
                        'ACCOUNT_NUMBER_QUALIFIER_RECEIVER',
                        'ACCOUNT_NUMBER_RECEIVER',
                        'PAYMENT_EFFECTIVE_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    BPR_HEADER,
                    TRANS_HANDLING_CODE,
                    TRANS_AMOUNT,
                    CREDIT_DEBIT_FLAG,
                    PAYMENT_METHOD_CODE,
                    PAYMENT_FORMAT_CODE,
                    DFI_ID_QUALIFIER_SENDER,
                    DFI_ID_SENDER,
                    ACCOUNT_NUMBER_QUALIFIER_SENDER,
                    ACCOUNT_NUMBER_SENDER,
                    BPR_ORIGINATING_COMPANY_ID,
                    ORIGINATING_COMPANY_SUPPLEMENTAL_CODE,
                    DFI_ID_QUALIFIER_RECEIVER,
                    DFI_ID_RECEIVER,
                    ACCOUNT_NUMBER_QUALIFIER_RECEIVER,
                    ACCOUNT_NUMBER_RECEIVER,
                    PAYMENT_EFFECTIVE_DATE
                )
)
, trn as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRN_HEADER'
                            when    flattened.index = 2   then      'TRACE_TYPE_CODE'
                            when    flattened.index = 3   then      'TRACE_ID'
                            when    flattened.index = 4   then      'TRN_ORIGINATING_COMPANY_ID'
                            when    flattened.index = 5   then      'SUPPLEMENTAL_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^TRN.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRN_HEADER',
                        'TRACE_TYPE_CODE',
                        'TRACE_ID',
                        'TRN_ORIGINATING_COMPANY_ID',
                        'SUPPLEMENTAL_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRN_HEADER,
                    TRACE_TYPE_CODE,
                    TRACE_ID,
                    TRN_ORIGINATING_COMPANY_ID,
                    SUPPLEMENTAL_ID
                )
)
, dtm_405 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_405_HEADER'
                            when    flattened.index = 2   then      'DTM_405_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_405_DATE'
                            when    flattened.index = 4   then      'DTM_405_TIME'
                            when    flattened.index = 5   then      'DTM_405_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_405_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_405_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*405.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_405_HEADER',
                        'DTM_405_QUALIFIER',
                        'DTM_405_DATE',
                        'DTM_405_TIME',
                        'DTM_405_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    DTM_405_HEADER,
                    DTM_405_QUALIFIER,
                    DTM_405_DATE,
                    DTM_405_TIME,
                    DTM_405_TIMEZONE
                )
)
, n1_pr as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_ID_CODE'
                            when    flattened.index = 3   then      'N1_PAYER_ORGANIZATION_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_ID_QUALIFIER'
                            when    flattened.index = 5   then      'N1_PAYER_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N1\\*PR.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_HEADER',
                        'N1_PAYER_ID_CODE',
                        'N1_PAYER_ORGANIZATION_NAME',
                        'N1_PAYER_ID_QUALIFIER',
                        'N1_PAYER_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_HEADER,
                    N1_PAYER_ID_CODE,
                    N1_PAYER_ORGANIZATION_NAME,
                    N1_PAYER_ID_QUALIFIER,
                    N1_PAYER_ID
                )
)
, n1_pr_n3 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_N3_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_N3_ADDRESS_L1'
                            when    flattened.index = 3   then      'N1_PAYER_N3_ADDRESS_L2'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N3.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_N3_HEADER',
                        'N1_PAYER_N3_ADDRESS_L1',
                        'N1_PAYER_N3_ADDRESS_L2'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_N3_HEADER,
                    N1_PAYER_N3_ADDRESS_L1,
                    N1_PAYER_N3_ADDRESS_L2
                )
)
, n1_pr_n4 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_N4_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_N4_CITY'
                            when    flattened.index = 3   then      'N1_PAYER_N4_ST'
                            when    flattened.index = 4   then      'N1_PAYER_N4_ZIP'
                            when    flattened.index = 5   then      'N1_PAYER_N4_COUNTRY'
                            when    flattened.index = 6   then      'N1_PAYER_N4_LOCATION_QUALIFIER'
                            when    flattened.index = 7   then      'N1_PAYER_N4_LOCATION_IDENTIFIER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N4.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_N4_HEADER',
                        'N1_PAYER_N4_CITY',
                        'N1_PAYER_N4_ST',
                        'N1_PAYER_N4_ZIP',
                        'N1_PAYER_N4_COUNTRY',
                        'N1_PAYER_N4_LOCATION_QUALIFIER',
                        'N1_PAYER_N4_LOCATION_IDENTIFIER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_N4_HEADER,
                    N1_PAYER_N4_CITY,
                    N1_PAYER_N4_ST,
                    N1_PAYER_N4_ZIP,
                    N1_PAYER_N4_COUNTRY,
                    N1_PAYER_N4_LOCATION_QUALIFIER,
                    N1_PAYER_N4_LOCATION_IDENTIFIER
                )
)
, n1_pr_per_bl as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_PER_BL_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_PER_BL_CONTACT_ROLE'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_PER_BL_CONTACT_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_PER_BL_QUALIFIER_1'
                            when    flattened.index = 5   then      'N1_PAYER_PER_BL_VALUE_1'
                            when    flattened.index = 6   then      'N1_PAYER_PER_BL_QUALIFIER_2'
                            when    flattened.index = 7   then      'N1_PAYER_PER_BL_VALUE_2'
                            when    flattened.index = 8   then      'N1_PAYER_PER_BL_QUALIFIER_3'
                            when    flattened.index = 9   then      'N1_PAYER_PER_BL_VALUE_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^PER\\*BL.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_PER_BL_HEADER',
                        'N1_PAYER_PER_BL_CONTACT_ROLE',
                        'N1_PAYER_PER_BL_CONTACT_NAME',
                        'N1_PAYER_PER_BL_QUALIFIER_1',
                        'N1_PAYER_PER_BL_VALUE_1',
                        'N1_PAYER_PER_BL_QUALIFIER_2',
                        'N1_PAYER_PER_BL_VALUE_2',
                        'N1_PAYER_PER_BL_QUALIFIER_3',
                        'N1_PAYER_PER_BL_VALUE_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_PER_BL_HEADER,
                    N1_PAYER_PER_BL_CONTACT_ROLE,
                    N1_PAYER_PER_BL_CONTACT_NAME,
                    N1_PAYER_PER_BL_QUALIFIER_1,
                    N1_PAYER_PER_BL_VALUE_1,
                    N1_PAYER_PER_BL_QUALIFIER_2,
                    N1_PAYER_PER_BL_VALUE_2,
                    N1_PAYER_PER_BL_QUALIFIER_3,
                    N1_PAYER_PER_BL_VALUE_3
                )
)
, n1_pr_per_cx as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_PER_CX_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_PER_CX_CONTACT_ROLE'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_PER_CX_CONTACT_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_PER_CX_QUALIFIER_1'
                            when    flattened.index = 5   then      'N1_PAYER_PER_CX_VALUE_1'
                            when    flattened.index = 6   then      'N1_PAYER_PER_CX_QUALIFIER_2'
                            when    flattened.index = 7   then      'N1_PAYER_PER_CX_VALUE_2'
                            when    flattened.index = 8   then      'N1_PAYER_PER_CX_QUALIFIER_3'
                            when    flattened.index = 9   then      'N1_PAYER_PER_CX_VALUE_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^PER\\*CX.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_PER_CX_HEADER',
                        'N1_PAYER_PER_CX_CONTACT_ROLE',
                        'N1_PAYER_PER_CX_CONTACT_NAME',
                        'N1_PAYER_PER_CX_QUALIFIER_1',
                        'N1_PAYER_PER_CX_VALUE_1',
                        'N1_PAYER_PER_CX_QUALIFIER_2',
                        'N1_PAYER_PER_CX_VALUE_2',
                        'N1_PAYER_PER_CX_QUALIFIER_3',
                        'N1_PAYER_PER_CX_VALUE_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_PER_CX_HEADER,
                    N1_PAYER_PER_CX_CONTACT_ROLE,
                    N1_PAYER_PER_CX_CONTACT_NAME,
                    N1_PAYER_PER_CX_QUALIFIER_1,
                    N1_PAYER_PER_CX_VALUE_1,
                    N1_PAYER_PER_CX_QUALIFIER_2,
                    N1_PAYER_PER_CX_VALUE_2,
                    N1_PAYER_PER_CX_QUALIFIER_3,
                    N1_PAYER_PER_CX_VALUE_3
                )
)
, n1_pr_ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_REF_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_REF_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_REF_REF_ID'
                            when    flattened.index = 4   then      'N1_PAYER_REF_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'N1_PAYER_REF_HEADER',
                            'N1_PAYER_REF_REF_ID_QUALIFIER',
                            'N1_PAYER_REF_REF_ID',
                            'N1_PAYER_REF_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        N1_PAYER_REF_HEADER,
                        N1_PAYER_REF_REF_ID_QUALIFIER,
                        N1_PAYER_REF_REF_ID,
                        N1_PAYER_REF_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                array_agg(
                    object_construct_keep_null(
                        'n1_payer_ref_code',           n1_payer_ref_ref_id_qualifier::varchar,
                        'n1_payer_ref_value',          n1_payer_ref_ref_id::varchar,
                        'n1_payer_ref_description',    n1_payer_ref_description::varchar
                    )
                )   as n1_payer_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, n1_pe as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_ID_CODE'
                            when    flattened.index = 3   then      'N1_PAYEE_ORGANIZATION_NAME'
                            when    flattened.index = 4   then      'N1_PAYEE_ID_QUALIFIER'
                            when    flattened.index = 5   then      'N1_PAYEE_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N1\\*PE.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_HEADER',
                        'N1_PAYEE_ID_CODE',
                        'N1_PAYEE_ORGANIZATION_NAME',
                        'N1_PAYEE_ID_QUALIFIER',
                        'N1_PAYEE_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_HEADER,
                    N1_PAYEE_ID_CODE,
                    N1_PAYEE_ORGANIZATION_NAME,
                    N1_PAYEE_ID_QUALIFIER,
                    N1_PAYEE_ID
                )
)
, n1_pe_n3 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_N3_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_N3_ADDRESS_L1'
                            when    flattened.index = 3   then      'N1_PAYEE_N3_ADDRESS_L2'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N3.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_N3_HEADER',
                        'N1_PAYEE_N3_ADDRESS_L1',
                        'N1_PAYEE_N3_ADDRESS_L2'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_N3_HEADER,
                    N1_PAYEE_N3_ADDRESS_L1,
                    N1_PAYEE_N3_ADDRESS_L2
                )
)
, n1_pe_n4 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_N4_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_N4_CITY'
                            when    flattened.index = 3   then      'N1_PAYEE_N4_ST'
                            when    flattened.index = 4   then      'N1_PAYEE_N4_ZIP'
                            when    flattened.index = 5   then      'N1_PAYEE_N4_COUNTRY'
                            when    flattened.index = 6   then      'N1_PAYEE_N4_LOCATION_QUALIFIER'
                            when    flattened.index = 7   then      'N1_PAYEE_N4_LOCATION_IDENTIFIER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N4.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_N4_HEADER',
                        'N1_PAYEE_N4_CITY',
                        'N1_PAYEE_N4_ST',
                        'N1_PAYEE_N4_ZIP',
                        'N1_PAYEE_N4_COUNTRY',
                        'N1_PAYEE_N4_LOCATION_QUALIFIER',
                        'N1_PAYEE_N4_LOCATION_IDENTIFIER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_N4_HEADER,
                    N1_PAYEE_N4_CITY,
                    N1_PAYEE_N4_ST,
                    N1_PAYEE_N4_ZIP,
                    N1_PAYEE_N4_COUNTRY,
                    N1_PAYEE_N4_LOCATION_QUALIFIER,
                    N1_PAYEE_N4_LOCATION_IDENTIFIER
                )
)
, n1_pe_ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_REF_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_REF_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYEE_REF_REF_ID'
                            when    flattened.index = 4   then      'N1_PAYEE_REF_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'N1_PAYEE_REF_HEADER',
                            'N1_PAYEE_REF_REF_ID_QUALIFIER',
                            'N1_PAYEE_REF_REF_ID',
                            'N1_PAYEE_REF_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        N1_PAYEE_REF_HEADER,
                        N1_PAYEE_REF_REF_ID_QUALIFIER,
                        N1_PAYEE_REF_REF_ID,
                        N1_PAYEE_REF_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                array_agg(
                    object_construct_keep_null(
                        'n1_payee_ref_code',           n1_payee_ref_ref_id_qualifier::varchar,
                        'n1_payee_ref_value',          n1_payee_ref_ref_id::varchar,
                        'n1_payee_ref_description',    n1_payee_ref_description::varchar
                    )
                )   as n1_payee_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
select      header_st.response_id,
            header_st.nth_functional_group,
            header_st.nth_transaction_set,
            header_st.transaction_set_header,
            header_st.transaction_set_id_code,
            header_st.transaction_set_control_number_header,
            header_st.implementation_convention_reference,
            trailer_se.transaction_set_trailer,
            trailer_se.transaction_segment_count,
            trailer_se.transaction_set_control_number_trailer,
            bpr.bpr_header,
            bpr.trans_handling_code,
            bpr.trans_amount,
            bpr.credit_debit_flag,
            bpr.payment_method_code,
            bpr.payment_format_code,
            bpr.dfi_id_qualifier_sender,
            bpr.dfi_id_sender,
            bpr.account_number_qualifier_sender,
            bpr.account_number_sender,
            bpr.bpr_originating_company_id,
            bpr.originating_company_supplemental_code,
            bpr.dfi_id_qualifier_receiver,
            bpr.dfi_id_receiver,
            bpr.account_number_qualifier_receiver,
            bpr.account_number_receiver,
            bpr.payment_effective_date,
            trn.trn_header,
            trn.trace_type_code,
            trn.trace_id,
            trn.trn_originating_company_id,
            trn.supplemental_id,
            dtm_405.dtm_405_header,
            dtm_405.dtm_405_qualifier,
            dtm_405.dtm_405_date,
            dtm_405.dtm_405_time,
            dtm_405.dtm_405_timezone,
            n1_pr.n1_payer_header,
            n1_pr.n1_payer_id_code,
            n1_pr.n1_payer_organization_name,
            n1_pr.n1_payer_id_qualifier,
            n1_pr.n1_payer_id,
            n1_pr_n3.n1_payer_n3_header,
            n1_pr_n3.n1_payer_n3_address_l1,
            n1_pr_n3.n1_payer_n3_address_l2,
            n1_pr_n4.n1_payer_n4_header,
            n1_pr_n4.n1_payer_n4_city,
            n1_pr_n4.n1_payer_n4_st,
            n1_pr_n4.n1_payer_n4_zip,
            n1_pr_n4.n1_payer_n4_country,
            n1_pr_n4.n1_payer_n4_location_qualifier,
            n1_pr_n4.n1_payer_n4_location_identifier,
            n1_pr_per_bl.n1_payer_per_bl_header,
            n1_pr_per_bl.n1_payer_per_bl_contact_role,
            n1_pr_per_bl.n1_payer_per_bl_contact_name,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_1,
            n1_pr_per_bl.n1_payer_per_bl_value_1,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_2,
            n1_pr_per_bl.n1_payer_per_bl_value_2,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_3,
            n1_pr_per_bl.n1_payer_per_bl_value_3,
            n1_pr_per_cx.n1_payer_per_cx_header,
            n1_pr_per_cx.n1_payer_per_cx_contact_role,
            n1_pr_per_cx.n1_payer_per_cx_contact_name,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_1,
            n1_pr_per_cx.n1_payer_per_cx_value_1,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_2,
            n1_pr_per_cx.n1_payer_per_cx_value_2,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_3,
            n1_pr_per_cx.n1_payer_per_cx_value_3,
            n1_pe.n1_payee_header,
            n1_pe.n1_payee_id_code,
            n1_pe.n1_payee_organization_name,
            n1_pe.n1_payee_id_qualifier,
            n1_pe.n1_payee_id,
            n1_pe_n3.n1_payee_n3_header,
            n1_pe_n3.n1_payee_n3_address_l1,
            n1_pe_n3.n1_payee_n3_address_l2,
            n1_pe_n4.n1_payee_n4_header,
            n1_pe_n4.n1_payee_n4_city,
            n1_pe_n4.n1_payee_n4_st,
            n1_pe_n4.n1_payee_n4_zip,
            n1_pe_n4.n1_payee_n4_country,
            n1_pe_n4.n1_payee_n4_location_qualifier,
            n1_pe_n4.n1_payee_n4_location_identifier,
            n1_pr_ref_array.n1_payer_ref_array,
            n1_pe_ref_array.n1_payee_ref_array

from        header_st
            left join
                trailer_se
                on  header_st.response_id           = trailer_se.response_id
                and header_st.nth_functional_group  = trailer_se.nth_functional_group
                and header_st.nth_transaction_set   = trailer_se.nth_transaction_set
            left join
                bpr
                on  header_st.response_id           = bpr.response_id
                and header_st.nth_functional_group  = bpr.nth_functional_group
                and header_st.nth_transaction_set   = bpr.nth_transaction_set
            left join
                trn
                on  header_st.response_id           = trn.response_id
                and header_st.nth_functional_group  = trn.nth_functional_group
                and header_st.nth_transaction_set   = trn.nth_transaction_set
            left join
                dtm_405
                on  header_st.response_id           = dtm_405.response_id
                and header_st.nth_functional_group  = dtm_405.nth_functional_group
                and header_st.nth_transaction_set   = dtm_405.nth_transaction_set
            left join
                n1_pr
                on  header_st.response_id           = n1_pr.response_id
                and header_st.nth_functional_group  = n1_pr.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr.nth_transaction_set
            left join
                n1_pr_n3
                on  header_st.response_id           = n1_pr_n3.response_id
                and header_st.nth_functional_group  = n1_pr_n3.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_n3.nth_transaction_set
            left join
                n1_pr_n4
                on  header_st.response_id           = n1_pr_n4.response_id
                and header_st.nth_functional_group  = n1_pr_n4.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_n4.nth_transaction_set
            left join
                n1_pr_per_bl
                on  header_st.response_id           = n1_pr_per_bl.response_id
                and header_st.nth_functional_group  = n1_pr_per_bl.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_per_bl.nth_transaction_set
            left join
                n1_pr_per_cx
                on  header_st.response_id           = n1_pr_per_cx.response_id
                and header_st.nth_functional_group  = n1_pr_per_cx.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_per_cx.nth_transaction_set
            left join
                n1_pr_ref_array
                on  header_st.response_id           = n1_pr_ref_array.response_id
                and header_st.nth_functional_group  = n1_pr_ref_array.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_ref_array.nth_transaction_set
            left join
                n1_pe
                on  header_st.response_id           = n1_pe.response_id
                and header_st.nth_functional_group  = n1_pe.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe.nth_transaction_set
            left join
                n1_pe_n3
                on  header_st.response_id           = n1_pe_n3.response_id
                and header_st.nth_functional_group  = n1_pe_n3.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_n3.nth_transaction_set
            left join
                n1_pe_n4
                on  header_st.response_id           = n1_pe_n4.response_id
                and header_st.nth_functional_group  = n1_pe_n4.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_n4.nth_transaction_set
            left join
                n1_pe_ref_array
                on  header_st.response_id           = n1_pe_ref_array.response_id
                and header_st.nth_functional_group  = n1_pe_ref_array.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_ref_array.nth_transaction_set
;



create or replace task
    edwprodhh.edi_835_parser.insert_transaction_sets
    warehouse = analysis_wh
    after edwprodhh.edi_835_parser.insert_response_flat
as
insert into
    edwprodhh.edi_835_parser.transaction_sets
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    TRANSACTION_SET_HEADER,
    TRANSACTION_SET_ID_CODE,
    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
    IMPLEMENTATION_CONVENTION_REFERENCE,
    TRANSACTION_SET_TRAILER,
    TRANSACTION_SEGMENT_COUNT,
    TRANSACTION_SET_CONTROL_NUMBER_TRAILER,
    BPR_HEADER,
    TRANS_HANDLING_CODE,
    TRANS_AMOUNT,
    CREDIT_DEBIT_FLAG,
    PAYMENT_METHOD_CODE,
    PAYMENT_FORMAT_CODE,
    DFI_ID_QUALIFIER_SENDER,
    DFI_ID_SENDER,
    ACCOUNT_NUMBER_QUALIFIER_SENDER,
    ACCOUNT_NUMBER_SENDER,
    BPR_ORIGINATING_COMPANY_ID,
    ORIGINATING_COMPANY_SUPPLEMENTAL_CODE,
    DFI_ID_QUALIFIER_RECEIVER,
    DFI_ID_RECEIVER,
    ACCOUNT_NUMBER_QUALIFIER_RECEIVER,
    ACCOUNT_NUMBER_RECEIVER,
    PAYMENT_EFFECTIVE_DATE,
    TRN_HEADER,
    TRACE_TYPE_CODE,
    TRACE_ID,
    TRN_ORIGINATING_COMPANY_ID,
    SUPPLEMENTAL_ID,
    DTM_405_HEADER,
    DTM_405_QUALIFIER,
    DTM_405_DATE,
    DTM_405_TIME,
    DTM_405_TIMEZONE,
    N1_PAYER_HEADER,
    N1_PAYER_ID_CODE,
    N1_PAYER_ORGANIZATION_NAME,
    N1_PAYER_ID_QUALIFIER,
    N1_PAYER_ID,
    N1_PAYER_N3_HEADER,
    N1_PAYER_N3_ADDRESS_L1,
    N1_PAYER_N3_ADDRESS_L2,
    N1_PAYER_N4_HEADER,
    N1_PAYER_N4_CITY,
    N1_PAYER_N4_ST,
    N1_PAYER_N4_ZIP,
    N1_PAYER_N4_COUNTRY,
    N1_PAYER_N4_LOCATION_QUALIFIER,
    N1_PAYER_N4_LOCATION_IDENTIFIER,
    N1_PAYER_PER_BL_HEADER,
    N1_PAYER_PER_BL_CONTACT_ROLE,
    N1_PAYER_PER_BL_CONTACT_NAME,
    N1_PAYER_PER_BL_QUALIFIER_1,
    N1_PAYER_PER_BL_VALUE_1,
    N1_PAYER_PER_BL_QUALIFIER_2,
    N1_PAYER_PER_BL_VALUE_2,
    N1_PAYER_PER_BL_QUALIFIER_3,
    N1_PAYER_PER_BL_VALUE_3,
    N1_PAYER_PER_CX_HEADER,
    N1_PAYER_PER_CX_CONTACT_ROLE,
    N1_PAYER_PER_CX_CONTACT_NAME,
    N1_PAYER_PER_CX_QUALIFIER_1,
    N1_PAYER_PER_CX_VALUE_1,
    N1_PAYER_PER_CX_QUALIFIER_2,
    N1_PAYER_PER_CX_VALUE_2,
    N1_PAYER_PER_CX_QUALIFIER_3,
    N1_PAYER_PER_CX_VALUE_3,
    N1_PAYEE_HEADER,
    N1_PAYEE_ID_CODE,
    N1_PAYEE_ORGANIZATION_NAME,
    N1_PAYEE_ID_QUALIFIER,
    N1_PAYEE_ID,
    N1_PAYEE_N3_HEADER,
    N1_PAYEE_N3_ADDRESS_L1,
    N1_PAYEE_N3_ADDRESS_L2,
    N1_PAYEE_N4_HEADER,
    N1_PAYEE_N4_CITY,
    N1_PAYEE_N4_ST,
    N1_PAYEE_N4_ZIP,
    N1_PAYEE_N4_COUNTRY,
    N1_PAYEE_N4_LOCATION_QUALIFIER,
    N1_PAYEE_N4_LOCATION_IDENTIFIER,
    N1_PAYER_REF_ARRAY,
    N1_PAYEE_REF_ARRAY
)
with filtered as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_835_parser.transaction_sets)
)
, header_st as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_HEADER'
                            when    flattened.index = 2   then      'TRANSACTION_SET_ID_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_HEADER'
                            when    flattened.index = 4   then      'IMPLEMENTATION_CONVENTION_REFERENCE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^ST.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_HEADER',
                        'TRANSACTION_SET_ID_CODE',
                        'TRANSACTION_SET_CONTROL_NUMBER_HEADER',
                        'IMPLEMENTATION_CONVENTION_REFERENCE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_HEADER,
                    TRANSACTION_SET_ID_CODE,
                    TRANSACTION_SET_CONTROL_NUMBER_HEADER,
                    IMPLEMENTATION_CONVENTION_REFERENCE
                )
)
, trailer_se as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRANSACTION_SET_TRAILER'
                            when    flattened.index = 2   then      'TRANSACTION_SEGMENT_COUNT'
                            when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^SE.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRANSACTION_SET_TRAILER',
                        'TRANSACTION_SEGMENT_COUNT',
                        'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRANSACTION_SET_TRAILER,
                    TRANSACTION_SEGMENT_COUNT,
                    TRANSACTION_SET_CONTROL_NUMBER_TRAILER
                )
)
, bpr as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BPR_HEADER'
                            when    flattened.index = 2   then      'TRANS_HANDLING_CODE'
                            when    flattened.index = 3   then      'TRANS_AMOUNT'
                            when    flattened.index = 4   then      'CREDIT_DEBIT_FLAG'
                            when    flattened.index = 5   then      'PAYMENT_METHOD_CODE'
                            when    flattened.index = 6   then      'PAYMENT_FORMAT_CODE'
                            when    flattened.index = 7   then      'DFI_ID_QUALIFIER_SENDER'
                            when    flattened.index = 8   then      'DFI_ID_SENDER'
                            when    flattened.index = 9   then      'ACCOUNT_NUMBER_QUALIFIER_SENDER'
                            when    flattened.index = 10  then      'ACCOUNT_NUMBER_SENDER'
                            when    flattened.index = 11  then      'BPR_ORIGINATING_COMPANY_ID'
                            when    flattened.index = 12  then      'ORIGINATING_COMPANY_SUPPLEMENTAL_CODE'
                            when    flattened.index = 13  then      'DFI_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 14  then      'DFI_ID_RECEIVER'
                            when    flattened.index = 15  then      'ACCOUNT_NUMBER_QUALIFIER_RECEIVER'
                            when    flattened.index = 16  then      'ACCOUNT_NUMBER_RECEIVER'
                            when    flattened.index = 17  then      'PAYMENT_EFFECTIVE_DATE'
                            end     as value_header,

                    case    when    value_header = 'PAYMENT_EFFECTIVE_DATE'     then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^BPR.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BPR_HEADER',
                        'TRANS_HANDLING_CODE',
                        'TRANS_AMOUNT',
                        'CREDIT_DEBIT_FLAG',
                        'PAYMENT_METHOD_CODE',
                        'PAYMENT_FORMAT_CODE',
                        'DFI_ID_QUALIFIER_SENDER',
                        'DFI_ID_SENDER',
                        'ACCOUNT_NUMBER_QUALIFIER_SENDER',
                        'ACCOUNT_NUMBER_SENDER',
                        'BPR_ORIGINATING_COMPANY_ID',
                        'ORIGINATING_COMPANY_SUPPLEMENTAL_CODE',
                        'DFI_ID_QUALIFIER_RECEIVER',
                        'DFI_ID_RECEIVER',
                        'ACCOUNT_NUMBER_QUALIFIER_RECEIVER',
                        'ACCOUNT_NUMBER_RECEIVER',
                        'PAYMENT_EFFECTIVE_DATE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    BPR_HEADER,
                    TRANS_HANDLING_CODE,
                    TRANS_AMOUNT,
                    CREDIT_DEBIT_FLAG,
                    PAYMENT_METHOD_CODE,
                    PAYMENT_FORMAT_CODE,
                    DFI_ID_QUALIFIER_SENDER,
                    DFI_ID_SENDER,
                    ACCOUNT_NUMBER_QUALIFIER_SENDER,
                    ACCOUNT_NUMBER_SENDER,
                    BPR_ORIGINATING_COMPANY_ID,
                    ORIGINATING_COMPANY_SUPPLEMENTAL_CODE,
                    DFI_ID_QUALIFIER_RECEIVER,
                    DFI_ID_RECEIVER,
                    ACCOUNT_NUMBER_QUALIFIER_RECEIVER,
                    ACCOUNT_NUMBER_RECEIVER,
                    PAYMENT_EFFECTIVE_DATE
                )
)
, trn as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'TRN_HEADER'
                            when    flattened.index = 2   then      'TRACE_TYPE_CODE'
                            when    flattened.index = 3   then      'TRACE_ID'
                            when    flattened.index = 4   then      'TRN_ORIGINATING_COMPANY_ID'
                            when    flattened.index = 5   then      'SUPPLEMENTAL_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^TRN.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'TRN_HEADER',
                        'TRACE_TYPE_CODE',
                        'TRACE_ID',
                        'TRN_ORIGINATING_COMPANY_ID',
                        'SUPPLEMENTAL_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    TRN_HEADER,
                    TRACE_TYPE_CODE,
                    TRACE_ID,
                    TRN_ORIGINATING_COMPANY_ID,
                    SUPPLEMENTAL_ID
                )
)
, dtm_405 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'DTM_405_HEADER'
                            when    flattened.index = 2   then      'DTM_405_QUALIFIER'
                            when    flattened.index = 3   then      'DTM_405_DATE'
                            when    flattened.index = 4   then      'DTM_405_TIME'
                            when    flattened.index = 5   then      'DTM_405_TIMEZONE'
                            end     as value_header,

                    case    when    value_header = 'DTM_405_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'DTM_405_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^DTM\\*405.*')                  --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'DTM_405_HEADER',
                        'DTM_405_QUALIFIER',
                        'DTM_405_DATE',
                        'DTM_405_TIME',
                        'DTM_405_TIMEZONE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    DTM_405_HEADER,
                    DTM_405_QUALIFIER,
                    DTM_405_DATE,
                    DTM_405_TIME,
                    DTM_405_TIMEZONE
                )
)
, n1_pr as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_ID_CODE'
                            when    flattened.index = 3   then      'N1_PAYER_ORGANIZATION_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_ID_QUALIFIER'
                            when    flattened.index = 5   then      'N1_PAYER_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N1\\*PR.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_HEADER',
                        'N1_PAYER_ID_CODE',
                        'N1_PAYER_ORGANIZATION_NAME',
                        'N1_PAYER_ID_QUALIFIER',
                        'N1_PAYER_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_HEADER,
                    N1_PAYER_ID_CODE,
                    N1_PAYER_ORGANIZATION_NAME,
                    N1_PAYER_ID_QUALIFIER,
                    N1_PAYER_ID
                )
)
, n1_pr_n3 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_N3_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_N3_ADDRESS_L1'
                            when    flattened.index = 3   then      'N1_PAYER_N3_ADDRESS_L2'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N3.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_N3_HEADER',
                        'N1_PAYER_N3_ADDRESS_L1',
                        'N1_PAYER_N3_ADDRESS_L2'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_N3_HEADER,
                    N1_PAYER_N3_ADDRESS_L1,
                    N1_PAYER_N3_ADDRESS_L2
                )
)
, n1_pr_n4 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_N4_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_N4_CITY'
                            when    flattened.index = 3   then      'N1_PAYER_N4_ST'
                            when    flattened.index = 4   then      'N1_PAYER_N4_ZIP'
                            when    flattened.index = 5   then      'N1_PAYER_N4_COUNTRY'
                            when    flattened.index = 6   then      'N1_PAYER_N4_LOCATION_QUALIFIER'
                            when    flattened.index = 7   then      'N1_PAYER_N4_LOCATION_IDENTIFIER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N4.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_N4_HEADER',
                        'N1_PAYER_N4_CITY',
                        'N1_PAYER_N4_ST',
                        'N1_PAYER_N4_ZIP',
                        'N1_PAYER_N4_COUNTRY',
                        'N1_PAYER_N4_LOCATION_QUALIFIER',
                        'N1_PAYER_N4_LOCATION_IDENTIFIER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_N4_HEADER,
                    N1_PAYER_N4_CITY,
                    N1_PAYER_N4_ST,
                    N1_PAYER_N4_ZIP,
                    N1_PAYER_N4_COUNTRY,
                    N1_PAYER_N4_LOCATION_QUALIFIER,
                    N1_PAYER_N4_LOCATION_IDENTIFIER
                )
)
, n1_pr_per_bl as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_PER_BL_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_PER_BL_CONTACT_ROLE'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_PER_BL_CONTACT_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_PER_BL_QUALIFIER_1'
                            when    flattened.index = 5   then      'N1_PAYER_PER_BL_VALUE_1'
                            when    flattened.index = 6   then      'N1_PAYER_PER_BL_QUALIFIER_2'
                            when    flattened.index = 7   then      'N1_PAYER_PER_BL_VALUE_2'
                            when    flattened.index = 8   then      'N1_PAYER_PER_BL_QUALIFIER_3'
                            when    flattened.index = 9   then      'N1_PAYER_PER_BL_VALUE_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^PER\\*BL.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_PER_BL_HEADER',
                        'N1_PAYER_PER_BL_CONTACT_ROLE',
                        'N1_PAYER_PER_BL_CONTACT_NAME',
                        'N1_PAYER_PER_BL_QUALIFIER_1',
                        'N1_PAYER_PER_BL_VALUE_1',
                        'N1_PAYER_PER_BL_QUALIFIER_2',
                        'N1_PAYER_PER_BL_VALUE_2',
                        'N1_PAYER_PER_BL_QUALIFIER_3',
                        'N1_PAYER_PER_BL_VALUE_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_PER_BL_HEADER,
                    N1_PAYER_PER_BL_CONTACT_ROLE,
                    N1_PAYER_PER_BL_CONTACT_NAME,
                    N1_PAYER_PER_BL_QUALIFIER_1,
                    N1_PAYER_PER_BL_VALUE_1,
                    N1_PAYER_PER_BL_QUALIFIER_2,
                    N1_PAYER_PER_BL_VALUE_2,
                    N1_PAYER_PER_BL_QUALIFIER_3,
                    N1_PAYER_PER_BL_VALUE_3
                )
)
, n1_pr_per_cx as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_PER_CX_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_PER_CX_CONTACT_ROLE'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_PER_CX_CONTACT_NAME'
                            when    flattened.index = 4   then      'N1_PAYER_PER_CX_QUALIFIER_1'
                            when    flattened.index = 5   then      'N1_PAYER_PER_CX_VALUE_1'
                            when    flattened.index = 6   then      'N1_PAYER_PER_CX_QUALIFIER_2'
                            when    flattened.index = 7   then      'N1_PAYER_PER_CX_VALUE_2'
                            when    flattened.index = 8   then      'N1_PAYER_PER_CX_QUALIFIER_3'
                            when    flattened.index = 9   then      'N1_PAYER_PER_CX_VALUE_3'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^PER\\*CX.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYER_PER_CX_HEADER',
                        'N1_PAYER_PER_CX_CONTACT_ROLE',
                        'N1_PAYER_PER_CX_CONTACT_NAME',
                        'N1_PAYER_PER_CX_QUALIFIER_1',
                        'N1_PAYER_PER_CX_VALUE_1',
                        'N1_PAYER_PER_CX_QUALIFIER_2',
                        'N1_PAYER_PER_CX_VALUE_2',
                        'N1_PAYER_PER_CX_QUALIFIER_3',
                        'N1_PAYER_PER_CX_VALUE_3'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYER_PER_CX_HEADER,
                    N1_PAYER_PER_CX_CONTACT_ROLE,
                    N1_PAYER_PER_CX_CONTACT_NAME,
                    N1_PAYER_PER_CX_QUALIFIER_1,
                    N1_PAYER_PER_CX_VALUE_1,
                    N1_PAYER_PER_CX_QUALIFIER_2,
                    N1_PAYER_PER_CX_VALUE_2,
                    N1_PAYER_PER_CX_QUALIFIER_3,
                    N1_PAYER_PER_CX_VALUE_3
                )
)
, n1_pr_ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYER_REF_HEADER'
                            when    flattened.index = 2   then      'N1_PAYER_REF_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYER_REF_REF_ID'
                            when    flattened.index = 4   then      'N1_PAYER_REF_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PR'
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'N1_PAYER_REF_HEADER',
                            'N1_PAYER_REF_REF_ID_QUALIFIER',
                            'N1_PAYER_REF_REF_ID',
                            'N1_PAYER_REF_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        N1_PAYER_REF_HEADER,
                        N1_PAYER_REF_REF_ID_QUALIFIER,
                        N1_PAYER_REF_REF_ID,
                        N1_PAYER_REF_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                array_agg(
                    object_construct_keep_null(
                        'n1_payer_ref_code',           n1_payer_ref_ref_id_qualifier::varchar,
                        'n1_payer_ref_value',          n1_payer_ref_ref_id::varchar,
                        'n1_payer_ref_description',    n1_payer_ref_description::varchar
                    )
                )   as n1_payer_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
, n1_pe as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_ID_CODE'
                            when    flattened.index = 3   then      'N1_PAYEE_ORGANIZATION_NAME'
                            when    flattened.index = 4   then      'N1_PAYEE_ID_QUALIFIER'
                            when    flattened.index = 5   then      'N1_PAYEE_ID'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N1\\*PE.*')                        --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_HEADER',
                        'N1_PAYEE_ID_CODE',
                        'N1_PAYEE_ORGANIZATION_NAME',
                        'N1_PAYEE_ID_QUALIFIER',
                        'N1_PAYEE_ID'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_HEADER,
                    N1_PAYEE_ID_CODE,
                    N1_PAYEE_ORGANIZATION_NAME,
                    N1_PAYEE_ID_QUALIFIER,
                    N1_PAYEE_ID
                )
)
, n1_pe_n3 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_N3_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_N3_ADDRESS_L1'
                            when    flattened.index = 3   then      'N1_PAYEE_N3_ADDRESS_L2'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N3.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_N3_HEADER',
                        'N1_PAYEE_N3_ADDRESS_L1',
                        'N1_PAYEE_N3_ADDRESS_L2'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_N3_HEADER,
                    N1_PAYEE_N3_ADDRESS_L1,
                    N1_PAYEE_N3_ADDRESS_L2
                )
)
, n1_pe_n4 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_N4_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_N4_CITY'
                            when    flattened.index = 3   then      'N1_PAYEE_N4_ST'
                            when    flattened.index = 4   then      'N1_PAYEE_N4_ZIP'
                            when    flattened.index = 5   then      'N1_PAYEE_N4_COUNTRY'
                            when    flattened.index = 6   then      'N1_PAYEE_N4_LOCATION_QUALIFIER'
                            when    flattened.index = 7   then      'N1_PAYEE_N4_LOCATION_IDENTIFIER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^N4.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'N1_PAYEE_N4_HEADER',
                        'N1_PAYEE_N4_CITY',
                        'N1_PAYEE_N4_ST',
                        'N1_PAYEE_N4_ZIP',
                        'N1_PAYEE_N4_COUNTRY',
                        'N1_PAYEE_N4_LOCATION_QUALIFIER',
                        'N1_PAYEE_N4_LOCATION_IDENTIFIER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    N1_PAYEE_N4_HEADER,
                    N1_PAYEE_N4_CITY,
                    N1_PAYEE_N4_ST,
                    N1_PAYEE_N4_ZIP,
                    N1_PAYEE_N4_COUNTRY,
                    N1_PAYEE_N4_LOCATION_QUALIFIER,
                    N1_PAYEE_N4_LOCATION_IDENTIFIER
                )
)
, n1_pe_ref_array as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'N1_PAYEE_REF_HEADER'
                            when    flattened.index = 2   then      'N1_PAYEE_REF_REF_ID_QUALIFIER'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                            when    flattened.index = 3   then      'N1_PAYEE_REF_REF_ID'
                            when    flattened.index = 4   then      'N1_PAYEE_REF_DESCRIPTION'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_835, '*') as flattened         --2 Flatten

        where       regexp_like(filtered.line_element_835, '^REF.*')                         --1 Filter
                    and filtered.lag_n1_indicator = 'N1*PE'
    )
    , pivoted as
    (
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'N1_PAYEE_REF_HEADER',
                            'N1_PAYEE_REF_REF_ID_QUALIFIER',
                            'N1_PAYEE_REF_REF_ID',
                            'N1_PAYEE_REF_DESCRIPTION'
                        )
                    )   as pvt (
                        RESPONSE_ID,
                        NTH_FUNCTIONAL_GROUP,
                        NTH_TRANSACTION_SET,
                        N1_PAYEE_REF_HEADER,
                        N1_PAYEE_REF_REF_ID_QUALIFIER,
                        N1_PAYEE_REF_REF_ID,
                        N1_PAYEE_REF_DESCRIPTION
                    )
    )
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                array_agg(
                    object_construct_keep_null(
                        'n1_payee_ref_code',           n1_payee_ref_ref_id_qualifier::varchar,
                        'n1_payee_ref_value',          n1_payee_ref_ref_id::varchar,
                        'n1_payee_ref_description',    n1_payee_ref_description::varchar
                    )
                )   as n1_payee_ref_array
    from        pivoted
    group by    1,2,3
    order by    1,2,3
)
select      header_st.response_id,
            header_st.nth_functional_group,
            header_st.nth_transaction_set,
            header_st.transaction_set_header,
            header_st.transaction_set_id_code,
            header_st.transaction_set_control_number_header,
            header_st.implementation_convention_reference,
            trailer_se.transaction_set_trailer,
            trailer_se.transaction_segment_count,
            trailer_se.transaction_set_control_number_trailer,
            bpr.bpr_header,
            bpr.trans_handling_code,
            bpr.trans_amount,
            bpr.credit_debit_flag,
            bpr.payment_method_code,
            bpr.payment_format_code,
            bpr.dfi_id_qualifier_sender,
            bpr.dfi_id_sender,
            bpr.account_number_qualifier_sender,
            bpr.account_number_sender,
            bpr.bpr_originating_company_id,
            bpr.originating_company_supplemental_code,
            bpr.dfi_id_qualifier_receiver,
            bpr.dfi_id_receiver,
            bpr.account_number_qualifier_receiver,
            bpr.account_number_receiver,
            bpr.payment_effective_date,
            trn.trn_header,
            trn.trace_type_code,
            trn.trace_id,
            trn.trn_originating_company_id,
            trn.supplemental_id,
            dtm_405.dtm_405_header,
            dtm_405.dtm_405_qualifier,
            dtm_405.dtm_405_date,
            dtm_405.dtm_405_time,
            dtm_405.dtm_405_timezone,
            n1_pr.n1_payer_header,
            n1_pr.n1_payer_id_code,
            n1_pr.n1_payer_organization_name,
            n1_pr.n1_payer_id_qualifier,
            n1_pr.n1_payer_id,
            n1_pr_n3.n1_payer_n3_header,
            n1_pr_n3.n1_payer_n3_address_l1,
            n1_pr_n3.n1_payer_n3_address_l2,
            n1_pr_n4.n1_payer_n4_header,
            n1_pr_n4.n1_payer_n4_city,
            n1_pr_n4.n1_payer_n4_st,
            n1_pr_n4.n1_payer_n4_zip,
            n1_pr_n4.n1_payer_n4_country,
            n1_pr_n4.n1_payer_n4_location_qualifier,
            n1_pr_n4.n1_payer_n4_location_identifier,
            n1_pr_per_bl.n1_payer_per_bl_header,
            n1_pr_per_bl.n1_payer_per_bl_contact_role,
            n1_pr_per_bl.n1_payer_per_bl_contact_name,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_1,
            n1_pr_per_bl.n1_payer_per_bl_value_1,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_2,
            n1_pr_per_bl.n1_payer_per_bl_value_2,
            n1_pr_per_bl.n1_payer_per_bl_qualifier_3,
            n1_pr_per_bl.n1_payer_per_bl_value_3,
            n1_pr_per_cx.n1_payer_per_cx_header,
            n1_pr_per_cx.n1_payer_per_cx_contact_role,
            n1_pr_per_cx.n1_payer_per_cx_contact_name,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_1,
            n1_pr_per_cx.n1_payer_per_cx_value_1,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_2,
            n1_pr_per_cx.n1_payer_per_cx_value_2,
            n1_pr_per_cx.n1_payer_per_cx_qualifier_3,
            n1_pr_per_cx.n1_payer_per_cx_value_3,
            n1_pe.n1_payee_header,
            n1_pe.n1_payee_id_code,
            n1_pe.n1_payee_organization_name,
            n1_pe.n1_payee_id_qualifier,
            n1_pe.n1_payee_id,
            n1_pe_n3.n1_payee_n3_header,
            n1_pe_n3.n1_payee_n3_address_l1,
            n1_pe_n3.n1_payee_n3_address_l2,
            n1_pe_n4.n1_payee_n4_header,
            n1_pe_n4.n1_payee_n4_city,
            n1_pe_n4.n1_payee_n4_st,
            n1_pe_n4.n1_payee_n4_zip,
            n1_pe_n4.n1_payee_n4_country,
            n1_pe_n4.n1_payee_n4_location_qualifier,
            n1_pe_n4.n1_payee_n4_location_identifier,
            n1_pr_ref_array.n1_payer_ref_array,
            n1_pe_ref_array.n1_payee_ref_array

from        header_st
            left join
                trailer_se
                on  header_st.response_id           = trailer_se.response_id
                and header_st.nth_functional_group  = trailer_se.nth_functional_group
                and header_st.nth_transaction_set   = trailer_se.nth_transaction_set
            left join
                bpr
                on  header_st.response_id           = bpr.response_id
                and header_st.nth_functional_group  = bpr.nth_functional_group
                and header_st.nth_transaction_set   = bpr.nth_transaction_set
            left join
                trn
                on  header_st.response_id           = trn.response_id
                and header_st.nth_functional_group  = trn.nth_functional_group
                and header_st.nth_transaction_set   = trn.nth_transaction_set
            left join
                dtm_405
                on  header_st.response_id           = dtm_405.response_id
                and header_st.nth_functional_group  = dtm_405.nth_functional_group
                and header_st.nth_transaction_set   = dtm_405.nth_transaction_set
            left join
                n1_pr
                on  header_st.response_id           = n1_pr.response_id
                and header_st.nth_functional_group  = n1_pr.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr.nth_transaction_set
            left join
                n1_pr_n3
                on  header_st.response_id           = n1_pr_n3.response_id
                and header_st.nth_functional_group  = n1_pr_n3.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_n3.nth_transaction_set
            left join
                n1_pr_n4
                on  header_st.response_id           = n1_pr_n4.response_id
                and header_st.nth_functional_group  = n1_pr_n4.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_n4.nth_transaction_set
            left join
                n1_pr_per_bl
                on  header_st.response_id           = n1_pr_per_bl.response_id
                and header_st.nth_functional_group  = n1_pr_per_bl.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_per_bl.nth_transaction_set
            left join
                n1_pr_per_cx
                on  header_st.response_id           = n1_pr_per_cx.response_id
                and header_st.nth_functional_group  = n1_pr_per_cx.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_per_cx.nth_transaction_set
            left join
                n1_pr_ref_array
                on  header_st.response_id           = n1_pr_ref_array.response_id
                and header_st.nth_functional_group  = n1_pr_ref_array.nth_functional_group
                and header_st.nth_transaction_set   = n1_pr_ref_array.nth_transaction_set
            left join
                n1_pe
                on  header_st.response_id           = n1_pe.response_id
                and header_st.nth_functional_group  = n1_pe.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe.nth_transaction_set
            left join
                n1_pe_n3
                on  header_st.response_id           = n1_pe_n3.response_id
                and header_st.nth_functional_group  = n1_pe_n3.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_n3.nth_transaction_set
            left join
                n1_pe_n4
                on  header_st.response_id           = n1_pe_n4.response_id
                and header_st.nth_functional_group  = n1_pe_n4.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_n4.nth_transaction_set
            left join
                n1_pe_ref_array
                on  header_st.response_id           = n1_pe_ref_array.response_id
                and header_st.nth_functional_group  = n1_pe_ref_array.nth_functional_group
                and header_st.nth_transaction_set   = n1_pe_ref_array.nth_transaction_set
;