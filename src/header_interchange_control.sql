create or replace table
    edwprodhh.edi_837_parser.header_interchange_control
as
with flatten_837 as
(
    with flattened_ as
    (
        select      response_id,
                    index,
                    trim(regexp_replace(value, '\\s+', ' ')) as line_element_837
        from        edwprodhh.edi_837_parser.response as response,
                    lateral split_to_table(response.response_body, '~') as flattened
    )
    select      *,
                count_if(regexp_like(line_element_837, '^ST.*')) over (partition by response_id order by index asc) as nth_transaction_set
    from        flattened_
)
, parse_interchange_control_header as
(
    with labeled as
    (
        select      flatten_837.response_id,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_HEADER'
                            when    flattened.index = 2   then      'AUTHORIZATION_INFORMATION_QUALIFIER'
                            when    flattened.index = 3   then      'AUTHORIZATION_INFORMATION'
                            when    flattened.index = 4   then      'SECURITY_INFORMATION_QUALIFIER'
                            when    flattened.index = 5   then      'SECURITY_INFORMATION'
                            when    flattened.index = 6   then      'INTERCHANGE_ID_QUALIFIER_SENDER'
                            when    flattened.index = 7   then      'INTERCHANGE_SENDER_ID'
                            when    flattened.index = 8   then      'INTERCHANGE_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 9   then      'INTERCHANGE_RECEIVER_ID'
                            when    flattened.index = 10  then      'INTERCHANGE_DATE'
                            when    flattened.index = 11  then      'INTERCHANGE_TIME'
                            when    flattened.index = 12  then      'REPETITION_SEPARATOR'
                            when    flattened.index = 13  then      'INTERCHANGE_CONTROL_VERSION'
                            when    flattened.index = 14  then      'INTERCHANGE_CONTROL_NUMBER'
                            when    flattened.index = 15  then      'ACKNOWLEDGEMENT_REQUESTED'
                            when    flattened.index = 16  then      'USAGE_INDICATOR'
                            when    flattened.index = 17  then      'COMPONENT_SEPARATOR'
                            end     as value_header,

                    case    when    value_header = 'INTERCHANGE_DATE'   then    to_date(nullif(trim(flattened.value), ''), 'YYMMDD')::text
                            when    value_header = 'INTERCHANGE_TIME'   then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
    )
    select      *,
                (interchange_date || ' ' || interchange_time)::timestamp as interchange_timestamp
    from        labeled
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_HEADER',
                        'AUTHORIZATION_INFORMATION_QUALIFIER',
                        'AUTHORIZATION_INFORMATION',
                        'SECURITY_INFORMATION_QUALIFIER',
                        'SECURITY_INFORMATION',
                        'INTERCHANGE_ID_QUALIFIER_SENDER',
                        'INTERCHANGE_SENDER_ID',
                        'INTERCHANGE_ID_QUALIFIER_RECEIVER',
                        'INTERCHANGE_RECEIVER_ID',
                        'INTERCHANGE_DATE',
                        'INTERCHANGE_TIME',
                        'REPETITION_SEPARATOR',
                        'INTERCHANGE_CONTROL_VERSION',
                        'INTERCHANGE_CONTROL_NUMBER',
                        'ACKNOWLEDGEMENT_REQUESTED',
                        'USAGE_INDICATOR',
                        'COMPONENT_SEPARATOR'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    INTERCHANGE_CONTROL_HEADER,
                    AUTHORIZATION_INFORMATION_QUALIFIER,
                    AUTHORIZATION_INFORMATION,
                    SECURITY_INFORMATION_QUALIFIER,
                    SECURITY_INFORMATION,
                    INTERCHANGE_ID_QUALIFIER_SENDER,
                    INTERCHANGE_SENDER_ID,
                    INTERCHANGE_ID_QUALIFIER_RECEIVER,
                    INTERCHANGE_RECEIVER_ID,
                    INTERCHANGE_DATE,
                    INTERCHANGE_TIME,
                    REPETITION_SEPARATOR,
                    INTERCHANGE_CONTROL_VERSION,
                    INTERCHANGE_CONTROL_NUMBER,
                    ACKNOWLEDGEMENT_REQUESTED,
                    USAGE_INDICATOR,
                    COMPONENT_SEPARATOR
                )
)
select      *
from        parse_interchange_control_header
;