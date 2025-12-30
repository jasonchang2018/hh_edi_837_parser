--FL for GS and ISA
with claims as
(
    select      response_id,
                nth_functional_group,
                nth_transaction_set,
                clp_claim_id as claim_id
    from        edwprodhh.edi_835_parser.remits
    where       response_id = '6e5e9a32c32b737b6119c7b620e8682e4f65a7fa963dac62a096477d56cab86b'
                and clp_claim_id in ('1322681550','1319557474')
)
, response_transaction_sets as
(
    select      response.response_id,
                response.nth_transaction_set,
                response.index,
                response.line_element_835,
    from        edwprodhh.edi_835_parser.response_flat as response
                inner join
                    claims
                    on  response.response_id            = claims.response_id
                    and response.nth_functional_group   = claims.nth_functional_group
                    and response.nth_transaction_set    = claims.nth_transaction_set
    where       not regexp_like(response.line_element_835, '^(ISA|IEA|GS|GE)\\*.*')
    order by    1,2
)
, response_functional_group as
(
    with claims_gs as
    (
        select      distinct
                    response_id,
                    nth_functional_group
        from        claims
    )
    , gs_header as
    (
        select      response.response_id,
                    response.nth_functional_group,
                    response.index,
                    response.line_element_835
        from        edwprodhh.edi_835_parser.response_flat as response
                    inner join
                        claims_gs
                        on  response.response_id            = claims_gs.response_id
                        and response.nth_functional_group   = claims_gs.nth_functional_group
        where       regexp_like(response.line_element_835, '^GS\\*.*')
        order by    1,2,3
    )
    , ge_trailer as
    (
        with mutate as
        (
            select      gs.response_id,
                        gs.nth_functional_group,
                        gs.index_ge,
                        gs.functional_group_trailer,
                        (select count(distinct nth_transaction_set) from response_transaction_sets) as ts_count_included,
                        gs.group_control_number
            from        edwprodhh.edi_835_parser.header_functional_group as gs
                        inner join
                            claims_gs
                            on  gs.response_id          = claims_gs.response_id
                            and gs.nth_functional_group = claims_gs.nth_functional_group
        )
        select      response_id,
                    nth_functional_group,
                    index_ge as index,
                    concat_ws('*', functional_group_trailer, ts_count_included, group_control_number) as line_element_835
        from        mutate
    )
    select      *
    from        gs_header
    union all
    select      *
    from        ge_trailer
)
, response_interchange as
(
    with claims_isa as
    (
        select      distinct
                    response_id,
                    nth_functional_group
        from        claims
    )
    , isa_header as
    (
        select      response.response_id,
                    response.index,
                    response.line_element_835
        from        edwprodhh.edi_835_parser.response_flat as response
                    inner join
                        claims_isa
                        on response.response_id = claims_isa.response_id
        where       regexp_like(response.line_element_835, '^ISA\\*.*')
    )
    , iea_trailer as
    (
        with mutate as
        (
            select      isa.response_id,
                        isa.index_iea,
                        isa.interchange_control_trailer,
                        (select count(distinct nth_functional_group) from response_functional_group) as gs_count_included,
                        isa.interchange_control_number_iea
            from        edwprodhh.edi_835_parser.header_interchange_control as isa
                        inner join
                            claims_isa
                            on isa.response_id = claims_isa.response_id
        )
        select      response_id,
                    index_iea as index,
                    concat_ws('*', interchange_control_trailer, gs_count_included, interchange_control_number_iea) as line_element_835
        from        mutate
    )
    select      *
    from        isa_header
    union all
    select      *
    from        iea_trailer
)
, unioned as
(
    select      response_id,
                index,
                line_element_835
    from        response_interchange
    union all
    select      response_id,
                index,
                line_element_835
    from        response_functional_group
    union all
    select      response_id,
                index,
                line_element_835
    from        response_transaction_sets
)
select      response_id,
            index,
            line_element_835 || '~' as line_element_835,
from        unioned
order by    1,2,3
;