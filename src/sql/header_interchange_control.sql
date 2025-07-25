create or replace table
    edwprodhh.edi_837_parser.header_interchange_control
as
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

    from        edwprodhh.edi_837_parser.response_flat as flatten_837,
                lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

    where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
)
select      response_id,
            interchange_control_header,
            authorization_information_qualifier,
            authorization_information,
            security_information_qualifier,
            security_information,
            interchange_id_qualifier_sender,
            interchange_sender_id,
            interchange_id_qualifier_receiver,
            interchange_receiver_id,
            interchange_date,
            interchange_time,
            repetition_separator,
            interchange_control_version,
            interchange_control_number,
            acknowledgement_requested,
            usage_indicator,
            component_separator,
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
order by    1
;



insert into
    edwprodhh.edi_837_parser.header_interchange_control
(
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
    COMPONENT_SEPARATOR,
    INTERCHANGE_TIMESTAMP
)
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

    from        edwprodhh.edi_837_parser.response_flat as flatten_837,
                lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

    where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
                and flatten_837.response_id not in (select response_id from edwprodhh.edi_837_parser.header_interchange_control)
)
select      response_id,
            interchange_control_header,
            authorization_information_qualifier,
            authorization_information,
            security_information_qualifier,
            security_information,
            interchange_id_qualifier_sender,
            interchange_sender_id,
            interchange_id_qualifier_receiver,
            interchange_receiver_id,
            interchange_date,
            interchange_time,
            repetition_separator,
            interchange_control_version,
            interchange_control_number,
            acknowledgement_requested,
            usage_indicator,
            component_separator,
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
order by    1
;