create or replace table
    edwprodhh.edi_277_parser.transaction_sets
as
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^ST\\*.*')                         --1 Filter
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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^SE.*')                         --1 Filter
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
, bht as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BHT_PREFIX'
                            when    flattened.index = 2   then      'HIERARCHICAL_STRUCTURE_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_PURPOSE_CODE'
                            when    flattened.index = 4   then      'ORIGINATOR_APPLICATION_TRANSACTION_ID'
                            when    flattened.index = 5   then      'TRANSACTION_SET_CREATED_DATE'
                            when    flattened.index = 6   then      'TRANSACTION_SET_CREATED_TIME'
                            when    flattened.index = 7   then      'TRANSACTION_TYPE_CODE'
                            end     as value_header,

                    case    when    value_header = 'TRANSACTION_SET_CREATED_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'TRANSACTION_SET_CREATED_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^BHT.*')                         --1 Filter
    )
    select      *,
                (TRANSACTION_SET_CREATED_DATE || ' ' || TRANSACTION_SET_CREATED_TIME)::timestamp as TRANSACTION_SET_CREATED_TIMESTAMP
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BHT_PREFIX',
                        'HIERARCHICAL_STRUCTURE_CODE',
                        'TRANSACTION_SET_PURPOSE_CODE',
                        'ORIGINATOR_APPLICATION_TRANSACTION_ID',
                        'TRANSACTION_SET_CREATED_DATE',
                        'TRANSACTION_SET_CREATED_TIME',
                        'TRANSACTION_TYPE_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    BHT_PREFIX,
                    HIERARCHICAL_STRUCTURE_CODE,
                    TRANSACTION_SET_PURPOSE_CODE,
                    ORIGINATOR_APPLICATION_TRANSACTION_ID,
                    TRANSACTION_SET_CREATED_DATE,
                    TRANSACTION_SET_CREATED_TIME,
                    TRANSACTION_TYPE_CODE
                )
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
            bht.bht_prefix,
            bht.hierarchical_structure_code,
            bht.transaction_set_purpose_code,
            bht.originator_application_transaction_id,
            bht.transaction_set_created_date,
            bht.transaction_set_created_time,
            bht.transaction_type_code,
            bht.transaction_set_created_timestamp

from        header_st
            left join
                trailer_se
                on  header_st.response_id           = trailer_se.response_id
                and header_st.nth_functional_group  = trailer_se.nth_functional_group
                and header_st.nth_transaction_set   = trailer_se.nth_transaction_set
            left join
                bht
                on  header_st.response_id           = bht.response_id
                and header_st.nth_functional_group  = bht.nth_functional_group
                and header_st.nth_transaction_set   = bht.nth_transaction_set
;



create or replace task
    edwprodhh.edi_277_parser.insert_transaction_sets
    warehouse = analysis_wh
    after edwprodhh.edi_277_parser.insert_response_flat
as
insert into
    edwprodhh.edi_277_parser.transaction_sets
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
    BHT_PREFIX,
    HIERARCHICAL_STRUCTURE_CODE,
    TRANSACTION_SET_PURPOSE_CODE,
    ORIGINATOR_APPLICATION_TRANSACTION_ID,
    TRANSACTION_SET_CREATED_DATE,
    TRANSACTION_SET_CREATED_TIME,
    TRANSACTION_TYPE_CODE,
    TRANSACTION_SET_CREATED_TIMESTAMP
)
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       response_id not in (select response_id from edwprodhh.edi_277_parser.transaction_sets)
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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^ST\\*.*')                         --1 Filter
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
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^SE.*')                         --1 Filter
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
, bht as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'BHT_PREFIX'
                            when    flattened.index = 2   then      'HIERARCHICAL_STRUCTURE_CODE'
                            when    flattened.index = 3   then      'TRANSACTION_SET_PURPOSE_CODE'
                            when    flattened.index = 4   then      'ORIGINATOR_APPLICATION_TRANSACTION_ID'
                            when    flattened.index = 5   then      'TRANSACTION_SET_CREATED_DATE'
                            when    flattened.index = 6   then      'TRANSACTION_SET_CREATED_TIME'
                            when    flattened.index = 7   then      'TRANSACTION_TYPE_CODE'
                            end     as value_header,

                    case    when    value_header = 'TRANSACTION_SET_CREATED_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'TRANSACTION_SET_CREATED_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^BHT.*')                         --1 Filter
    )
    select      *,
                (TRANSACTION_SET_CREATED_DATE || ' ' || TRANSACTION_SET_CREATED_TIME)::timestamp as TRANSACTION_SET_CREATED_TIMESTAMP
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'BHT_PREFIX',
                        'HIERARCHICAL_STRUCTURE_CODE',
                        'TRANSACTION_SET_PURPOSE_CODE',
                        'ORIGINATOR_APPLICATION_TRANSACTION_ID',
                        'TRANSACTION_SET_CREATED_DATE',
                        'TRANSACTION_SET_CREATED_TIME',
                        'TRANSACTION_TYPE_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    BHT_PREFIX,
                    HIERARCHICAL_STRUCTURE_CODE,
                    TRANSACTION_SET_PURPOSE_CODE,
                    ORIGINATOR_APPLICATION_TRANSACTION_ID,
                    TRANSACTION_SET_CREATED_DATE,
                    TRANSACTION_SET_CREATED_TIME,
                    TRANSACTION_TYPE_CODE
                )
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
            bht.bht_prefix,
            bht.hierarchical_structure_code,
            bht.transaction_set_purpose_code,
            bht.originator_application_transaction_id,
            bht.transaction_set_created_date,
            bht.transaction_set_created_time,
            bht.transaction_type_code,
            bht.transaction_set_created_timestamp

from        header_st
            left join
                trailer_se
                on  header_st.response_id           = trailer_se.response_id
                and header_st.nth_functional_group  = trailer_se.nth_functional_group
                and header_st.nth_transaction_set   = trailer_se.nth_transaction_set
            left join
                bht
                on  header_st.response_id           = bht.response_id
                and header_st.nth_functional_group  = bht.nth_functional_group
                and header_st.nth_transaction_set   = bht.nth_transaction_set
;