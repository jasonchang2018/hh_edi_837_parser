create or replace table
    edwprodhh.edi_277_parser.hl_19_subscriber
as
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = '19'
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

                    case    when    flattened.index = 1   then      'HL_19_PREFIX'
                            when    flattened.index = 2   then      'HL_19_ID'
                            when    flattened.index = 3   then      'HL_19_PARENT_ID'
                            when    flattened.index = 4   then      'HL_19_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_19_CHILD_CODE'
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
                        'HL_19_PREFIX',
                        'HL_19_ID',
                        'HL_19_PARENT_ID',
                        'HL_19_LEVEL_CODE',
                        'HL_19_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_19_PREFIX,
                    HL_19_ID,
                    HL_19_PARENT_ID,
                    HL_19_LEVEL_CODE,
                    HL_19_CHILD_CODE
                )
)
, nm1_85 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_85_SUBSCRIBER_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_85_SUBSCRIBER_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_85_SUBSCRIBER_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_85_SUBSCRIBER_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_85_SUBSCRIBER_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_85_SUBSCRIBER_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_85_SUBSCRIBER_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*85.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_85_SUBSCRIBER_NAME_CODE',
                        'NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE',
                        'NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER',
                        'NM1_85_SUBSCRIBER_LAST_NAME_ORG',
                        'NM1_85_SUBSCRIBER_FIRST_NAME',
                        'NM1_85_SUBSCRIBER_MIDDLE_NAME',
                        'NM1_85_SUBSCRIBER_NAME_PREFIX',
                        'NM1_85_SUBSCRIBER_NAME_SUFFIX',
                        'NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER',
                        'NM1_85_SUBSCRIBER_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    NM1_85_SUBSCRIBER_NAME_CODE,
                    NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE,
                    NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER,
                    NM1_85_SUBSCRIBER_LAST_NAME_ORG,
                    NM1_85_SUBSCRIBER_FIRST_NAME,
                    NM1_85_SUBSCRIBER_MIDDLE_NAME,
                    NM1_85_SUBSCRIBER_NAME_PREFIX,
                    NM1_85_SUBSCRIBER_NAME_SUFFIX,
                    NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER,
                    NM1_85_SUBSCRIBER_ID_CODE
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_19_prefix,
            hl.hl_19_id,
            hl.hl_19_parent_id,
            hl.hl_19_level_code,
            hl.hl_19_child_code,
            nm1_85.nm1_85_subscriber_name_code,
            nm1_85.nm1_85_subscriber_entity_identifier_code,
            nm1_85.nm1_85_subscriber_entity_type_qualifier,
            nm1_85.nm1_85_subscriber_last_name_org,
            nm1_85.nm1_85_subscriber_first_name,
            nm1_85.nm1_85_subscriber_middle_name,
            nm1_85.nm1_85_subscriber_name_prefix,
            nm1_85.nm1_85_subscriber_name_suffix,
            nm1_85.nm1_85_subscriber_id_code_qualifier,
            nm1_85.nm1_85_subscriber_id_code
from        hl
            left join
                nm1_85
                on  hl.response_id          = nm1_85.response_id
                and hl.nth_functional_group = nm1_85.nth_functional_group
                and hl.nth_transaction_set  = nm1_85.nth_transaction_set
                and hl.hl_index             = nm1_85.hl_index
;



create or replace task
    edwprodhh.edi_277_parser.insert_hl_19_subscriber
    warehouse = analysis_wh
    after edwprodhh.edi_277_parser.insert_response_flat
as
insert into
    edwprodhh.edi_277_parser.hl_19_subscriber
(
    RESPONSE_ID,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    HL_INDEX,
    HL_19_PREFIX,
    HL_19_ID,
    HL_19_PARENT_ID,
    HL_19_LEVEL_CODE,
    HL_19_CHILD_CODE,
    NM1_85_SUBSCRIBER_NAME_CODE,
    NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE,
    NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER,
    NM1_85_SUBSCRIBER_LAST_NAME_ORG,
    NM1_85_SUBSCRIBER_FIRST_NAME,
    NM1_85_SUBSCRIBER_MIDDLE_NAME,
    NM1_85_SUBSCRIBER_NAME_PREFIX,
    NM1_85_SUBSCRIBER_NAME_SUFFIX,
    NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER,
    NM1_85_SUBSCRIBER_ID_CODE
)
with filtered as
(
    select      *
    from        edwprodhh.edi_277_parser.response_flat
    where       hl_index is not null
                and lag_hl_indicator = '19'
                and response_id not in (select response_id from edwprodhh.edi_277_parser.hl_19_subscriber)
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

                    case    when    flattened.index = 1   then      'HL_19_PREFIX'
                            when    flattened.index = 2   then      'HL_19_ID'
                            when    flattened.index = 3   then      'HL_19_PARENT_ID'
                            when    flattened.index = 4   then      'HL_19_LEVEL_CODE'
                            when    flattened.index = 5   then      'HL_19_CHILD_CODE'
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
                        'HL_19_PREFIX',
                        'HL_19_ID',
                        'HL_19_PARENT_ID',
                        'HL_19_LEVEL_CODE',
                        'HL_19_CHILD_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    HL_19_PREFIX,
                    HL_19_ID,
                    HL_19_PARENT_ID,
                    HL_19_LEVEL_CODE,
                    HL_19_CHILD_CODE
                )
)
, nm1_85 as
(
    with long as
    (
        select      filtered.response_id,
                    filtered.nth_functional_group,
                    filtered.nth_transaction_set,
                    filtered.hl_index,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'NM1_85_SUBSCRIBER_NAME_CODE'
                            when    flattened.index = 2   then      'NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE'
                            when    flattened.index = 3   then      'NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER'
                            when    flattened.index = 4   then      'NM1_85_SUBSCRIBER_LAST_NAME_ORG'
                            when    flattened.index = 5   then      'NM1_85_SUBSCRIBER_FIRST_NAME'
                            when    flattened.index = 6   then      'NM1_85_SUBSCRIBER_MIDDLE_NAME'
                            when    flattened.index = 7   then      'NM1_85_SUBSCRIBER_NAME_PREFIX'
                            when    flattened.index = 8   then      'NM1_85_SUBSCRIBER_NAME_SUFFIX'
                            when    flattened.index = 9   then      'NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER'
                            when    flattened.index = 10  then      'NM1_85_SUBSCRIBER_ID_CODE'
                            end     as value_header,

                    nullif(trim(flattened.value), '') as value_format

        from        filtered,
                    lateral split_to_table(filtered.line_element_277, '*') as flattened     --2 Flatten

        where       regexp_like(filtered.line_element_277, '^NM1\\*85.*')                         --1 Filter
    )
    select      *
    from        long
                pivot(
                    max(value_format) for value_header in (
                        'NM1_85_SUBSCRIBER_NAME_CODE',
                        'NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE',
                        'NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER',
                        'NM1_85_SUBSCRIBER_LAST_NAME_ORG',
                        'NM1_85_SUBSCRIBER_FIRST_NAME',
                        'NM1_85_SUBSCRIBER_MIDDLE_NAME',
                        'NM1_85_SUBSCRIBER_NAME_PREFIX',
                        'NM1_85_SUBSCRIBER_NAME_SUFFIX',
                        'NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER',
                        'NM1_85_SUBSCRIBER_ID_CODE'
                    )
                )   as pvt (
                    RESPONSE_ID,
                    NTH_FUNCTIONAL_GROUP,
                    NTH_TRANSACTION_SET,
                    HL_INDEX,
                    NM1_85_SUBSCRIBER_NAME_CODE,
                    NM1_85_SUBSCRIBER_ENTITY_IDENTIFIER_CODE,
                    NM1_85_SUBSCRIBER_ENTITY_TYPE_QUALIFIER,
                    NM1_85_SUBSCRIBER_LAST_NAME_ORG,
                    NM1_85_SUBSCRIBER_FIRST_NAME,
                    NM1_85_SUBSCRIBER_MIDDLE_NAME,
                    NM1_85_SUBSCRIBER_NAME_PREFIX,
                    NM1_85_SUBSCRIBER_NAME_SUFFIX,
                    NM1_85_SUBSCRIBER_ID_CODE_QUALIFIER,
                    NM1_85_SUBSCRIBER_ID_CODE
                )
)
select      hl.response_id,
            hl.nth_functional_group,
            hl.nth_transaction_set,
            hl.hl_index,
            hl.hl_19_prefix,
            hl.hl_19_id,
            hl.hl_19_parent_id,
            hl.hl_19_level_code,
            hl.hl_19_child_code,
            nm1_85.nm1_85_subscriber_name_code,
            nm1_85.nm1_85_subscriber_entity_identifier_code,
            nm1_85.nm1_85_subscriber_entity_type_qualifier,
            nm1_85.nm1_85_subscriber_last_name_org,
            nm1_85.nm1_85_subscriber_first_name,
            nm1_85.nm1_85_subscriber_middle_name,
            nm1_85.nm1_85_subscriber_name_prefix,
            nm1_85.nm1_85_subscriber_name_suffix,
            nm1_85.nm1_85_subscriber_id_code_qualifier,
            nm1_85.nm1_85_subscriber_id_code
from        hl
            left join
                nm1_85
                on  hl.response_id          = nm1_85.response_id
                and hl.nth_functional_group = nm1_85.nth_functional_group
                and hl.nth_transaction_set  = nm1_85.nth_transaction_set
                and hl.hl_index             = nm1_85.hl_index
;