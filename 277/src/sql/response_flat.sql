create or replace table
    edwprodhh.edi_277_parser.response_flat
as
with flattened as
(
    select      response_id,
                to_date(regexp_substr(file_name, '\\-(\\d{8})', 1, 1, 'e'), 'yyyymmdd')     as file_date,
                line_number                                                                 as index,
                trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))   as line_element_277
    from        edwprodhh.edi_277_parser.response
)
, add_functional_group_index as
(  
    select      *,

                count_if(regexp_like(line_element_277, '^GS.*')) over (partition by response_id order by index asc)
                    as nth_functional_group

    from        flattened
)
, add_transaction_set_index as
(  
    select      *,

                count_if(regexp_like(line_element_277, '^ST\\*.*')) over (partition by response_id, nth_functional_group order by index asc)
                    as nth_transaction_set

    from        add_functional_group_index
)
, add_hl_index as
(
    select      *,
                max(case when regexp_like(line_element_277, '^HL.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index,
                
                coalesce(
                    regexp_substr(line_element_277, '^HL\\*[^\\*]*\\*[^\\*]*\\*([^\\*]*)', 1, 1, 'e'),
                    lag(case when regexp_like(line_element_277, '^HL.*') then regexp_substr(line_element_277, '^HL\\*[^\\*]*\\*[^\\*]*\\*([^\\*]*)', 1, 1, 'e') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                )   as lag_hl_indicator
                
    from        add_transaction_set_index
)
select      *
from        add_hl_index
;



create or replace task
    edwprodhh.edi_277_parser.insert_response_flat
    warehouse = analysis_wh
    after edwprodhh.edi_277_parser.sp_insert_277_from_stage
as
insert into
    edwprodhh.edi_277_parser.response_flat
(
    RESPONSE_ID,
    FILE_DATE,
    INDEX,
    LINE_ELEMENT_277,
    NTH_FUNCTIONAL_GROUP,
    NTH_TRANSACTION_SET,
    HL_INDEX,
    LAG_HL_INDICATOR
)
with flattened as
(
    select      response_id,
                to_date(regexp_substr(file_name, '\\-(\\d{8})', 1, 1, 'e'), 'yyyymmdd')     as file_date,
                line_number                                                                 as index,
                trim(regexp_replace(regexp_replace(response_body, '\\s+', ' '), '~', ''))   as line_element_277
    from        edwprodhh.edi_277_parser.response
    where       response_id not in (select response_id from edwprodhh.edi_277_parser.response_flat)
)
, add_functional_group_index as
(  
    select      *,

                count_if(regexp_like(line_element_277, '^GS.*')) over (partition by response_id order by index asc)
                    as nth_functional_group

    from        flattened
)
, add_transaction_set_index as
(  
    select      *,

                count_if(regexp_like(line_element_277, '^ST\\*.*')) over (partition by response_id, nth_functional_group order by index asc)
                    as nth_transaction_set

    from        add_functional_group_index
)
, add_hl_index as
(
    select      *,
                max(case when regexp_like(line_element_277, '^HL.*') then index end) over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc) as hl_index,
                
                coalesce(
                    regexp_substr(line_element_277, '^HL\\*[^\\*]*\\*[^\\*]*\\*([^\\*]*)', 1, 1, 'e'),
                    lag(case when regexp_like(line_element_277, '^HL.*') then regexp_substr(line_element_277, '^HL\\*[^\\*]*\\*[^\\*]*\\*([^\\*]*)', 1, 1, 'e') end) ignore nulls over (partition by response_id, nth_functional_group, nth_transaction_set order by index asc)
                )   as lag_hl_indicator
                
    from        add_transaction_set_index
)
select      *
from        add_hl_index
;