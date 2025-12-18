create or replace table
    edwprodhh.edi_835_parser.header_functional_group
as
with header_gs as
(
    with long as
    (
        select      flatten_835.response_id,
                    flatten_835.nth_functional_group,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_HEADER'
                            when    flattened.index = 2     then    'FUNCTIONAL_IDENTIFIER_CODE'
                            when    flattened.index = 3     then    'APPLICATION_SENDER_CODE'
                            when    flattened.index = 4     then    'APPLICATION_RECEIVER_CODE'
                            when    flattened.index = 5     then    'FUNCTIONAL_GROUP_CREATED_DATE'
                            when    flattened.index = 6     then    'FUNCTIONAL_GROUP_CREATED_TIME'
                            when    flattened.index = 7     then    'CONTROL_GROUP_NUMBER'
                            when    flattened.index = 8     then    'RESPONSIBLE_AGENCY_CODE'
                            when    flattened.index = 9     then    'VERSION_IDENTIFIER_CODE'
                            end     as value_header,

                    case    when    value_header = 'FUNCTIONAL_GROUP_CREATED_DATE'
                            then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'FUNCTIONAL_GROUP_CREATED_TIME'
                            then    case    when    length(nullif(trim(flattened.value), '')) = 6
                                            then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                            else    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                                            end
                            else    nullif(trim(flattened.value), '')
                            end     as value_format


        from        edwprodhh.edi_835_parser.response_flat as flatten_835,
                    lateral split_to_table(flatten_835.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_835.line_element_835, '^GS.*')                          --1 Filter
    )
    select      *,
                (functional_group_created_date || ' ' || functional_group_created_time)::timestamp as functional_group_created_timestamp
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_HEADER',
                        'FUNCTIONAL_IDENTIFIER_CODE',
                        'APPLICATION_SENDER_CODE',
                        'APPLICATION_RECEIVER_CODE',
                        'FUNCTIONAL_GROUP_CREATED_DATE',
                        'FUNCTIONAL_GROUP_CREATED_TIME',
                        'CONTROL_GROUP_NUMBER',
                        'RESPONSIBLE_AGENCY_CODE',
                        'VERSION_IDENTIFIER_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    FUNCTIONAL_GROUP_HEADER,
                    FUNCTIONAL_IDENTIFIER_CODE,
                    APPLICATION_SENDER_CODE,
                    APPLICATION_RECEIVER_CODE,
                    FUNCTIONAL_GROUP_CREATED_DATE,
                    FUNCTIONAL_GROUP_CREATED_TIME,
                    CONTROL_GROUP_NUMBER,
                    RESPONSIBLE_AGENCY_CODE,
                    VERSION_IDENTIFIER_CODE
                )
    order by    1
)
, trailer_ge as
(
    with long as
    (
        select      flatten_835.response_id,
                    flatten_835.nth_functional_group,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_TRAILER'
                            when    flattened.index = 2     then    'TS_COUNT_INCLUDED'
                            when    flattened.index = 3     then    'GROUP_CONTROL_NUMBER'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format


        from        edwprodhh.edi_835_parser.response_flat as flatten_835,
                    lateral split_to_table(flatten_835.line_element_835, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_835.line_element_835, '^GE.*')                          --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_TRAILER',
                        'TS_COUNT_INCLUDED',
                        'GROUP_CONTROL_NUMBER'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    FUNCTIONAL_GROUP_TRAILER,
                    TS_COUNT_INCLUDED,
                    GROUP_CONTROL_NUMBER
                )
    order by    1
)
select      header_gs.response_id,
            header_gs.nth_functional_group,
            header_gs.functional_group_header,
            header_gs.functional_identifier_code,
            header_gs.application_sender_code,
            header_gs.application_receiver_code,
            header_gs.functional_group_created_date,
            header_gs.functional_group_created_time,
            header_gs.control_group_number,
            header_gs.responsible_agency_code,
            header_gs.version_identifier_code,
            header_gs.functional_group_created_timestamp,
            trailer_ge.functional_group_trailer,
            trailer_ge.ts_count_included,
            trailer_ge.group_control_number
from        header_gs
            left join
                trailer_ge
                on  header_gs.response_id           = trailer_ge.response_id
                and header_gs.nth_functional_group  = trailer_ge.nth_functional_group
;



-- create or replace task
--     edwprodhh.edi_835_parser.insert_header_functional_group
--     warehouse = analysis_wh
--     after edwprodhh.edi_835_parser.insert_response_flat
-- as
-- insert into
--     edwprodhh.edi_835_parser.header_functional_group
-- (
--     RESPONSE_ID,
--     NTH_FUNCTIONAL_GROUP,
--     FUNCTIONAL_GROUP_HEADER,
--     FUNCTIONAL_IDENTIFIER_CODE,
--     APPLICATION_SENDER_CODE,
--     APPLICATION_RECEIVER_CODE,
--     FUNCTIONAL_GROUP_CREATED_DATE,
--     FUNCTIONAL_GROUP_CREATED_TIME,
--     CONTROL_GROUP_NUMBER,
--     RESPONSIBLE_AGENCY_CODE,
--     VERSION_IDENTIFIER_CODE,
--     FUNCTIONAL_GROUP_CREATED_TIMESTAMP
-- )
-- -- and flatten_835.response_id not in (select response_id from edwprodhh.edi_835_parser.header_functional_group)