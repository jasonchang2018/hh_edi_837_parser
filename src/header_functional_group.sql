create or replace table
    edwprodhh.edi_837_parser.header_functional_group
as
with labeled as
(
    select      flatten_837.response_id,
    
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

                case    when    value_header = 'FUNCTIONAL_GROUP_CREATED_DATE'  then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                        when    value_header = 'FUNCTIONAL_GROUP_CREATED_TIME'  then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                        else    nullif(trim(flattened.value), '')
                        end     as value_format


    from        edwprodhh.edi_837_parser.response_flat as flatten_837,
                lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

    where       regexp_like(flatten_837.line_element_837, '^GS.*')                          --1 Filter
)
select      *,
            (functional_group_created_date || ' ' || functional_group_created_time)::timestamp as functional_group_created_timestamp
from        labeled
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
;