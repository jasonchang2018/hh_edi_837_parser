 --*logic to determine which response ID to use
  --*is there a way to determine the first one proactively? using how the 835 or 837 is formatted. analyze the 3 vs 11 of the sample 14
  --*improve match using date + qualify?
  --SE count tag

create table
    edwprodhh.edi_835_parser.export_splicer_log
(
    RESPONSE_ID         VARCHAR(16777216),
    INDEX               NUMBER(38,0),
    LINE_ELEMENT_835    VARCHAR(16777216),
    IS_POSTED           NUMBER(1,0),
    UPLOAD_DATE         DATE
)
;


-- create or replace task
--     edwprodhh.edi_835_parser.insert_export_splicer_log
--     warehouse = analysis_wh
--     schedule = 'USING CRON 0 4 * * MON-FRI America/Chicago'
-- as
insert into
    edwprodhh.edi_835_parser.export_splicer_log
(
    RESPONSE_ID,
    INDEX,
    LINE_ELEMENT_835,
    IS_POSTED,
    UPLOAD_DATE
)
with response as
(
    select      *
    from        edwprodhh.edi_835_parser.response_flat
    where       response_id = '6e5e9a32c32b737b6119c7b620e8682e4f65a7fa963dac62a096477d56cab86b' --*
)
, claims_posted as
(
    with claims_all as
    (
        select      response_id,
                    nth_functional_group,
                    nth_transaction_set,
                    clp_claim_id                                                        as claim_id,
                    regexp_substr(ltrim(clp_claim_id, '0'), '(\\d*$)', 1, 1, 'e')       as claim_id_format,
                    clp_claim_payment_amount::number(18,2)                              as claim_payment_amount
        from        edwprodhh.edi_835_parser.remits
        where       response_id in (select response_id from response)
    )
    , posted_cubs as
    (
        /*Inflates match when multiple payments of same amount. 
            Assumes cubs posts at claim level and not at service line level.*/ --*
        select      remits.claim_id,
                    max(case when trans.trans_idx is not null then 1 else 0 end) as is_posted_cubs
        from        claims_all as remits
                    left join
                        edwprodhh.pub_jchang.master_debtor as debtor
                        on remits.claim_id_format = debtor.client_debtornumber
                        and debtor.pl_group = 'IU HEALTH - TPL'
                    left join
                        edwprodhh.pub_jchang.master_transactions as trans
                        on  debtor.debtor_idx = trans.debtor_idx
                        and trans.is_payment = 1
                        and remits.claim_payment_amount = trans.sig_trans_amt
        group by    1
        order by    1
    )
    select      *
    from        claims_all
    -- where       claim_id in (select claim_id from posted_cubs where is_posted_cubs = 1)
    where       claim_id in ('1319557474', '03I111391552')
)
, response_transaction_sets as
(
    select      response.response_id,
                response.nth_functional_group,
                response.nth_transaction_set,
                response.index,
                response.line_element_835,
                case when claims_posted.response_id is not null then 1 else 0 end as is_posted
    from        response
                left join
                    claims_posted
                    on  response.response_id            = claims_posted.response_id
                    and response.nth_functional_group   = claims_posted.nth_functional_group
                    and response.nth_transaction_set    = claims_posted.nth_transaction_set
    where       not regexp_like(response.line_element_835, '^(ISA|IEA|GS|GE)\\*.*')
    order by    1,2
)
, response_functional_group as
(
    with posted as
    (
        with target_gs as
        (
            select      response_id,
                        nth_functional_group,
                        count(distinct nth_transaction_set) as n_transaction_sets
            from        response_transaction_sets
            where       is_posted = 1
            group by    1,2
        )
        , gs_header as
        (
            select      response.response_id,
                        response.nth_functional_group,
                        response.index,
                        response.line_element_835
            from        response
                        inner join
                            target_gs
                            on  response.response_id            = target_gs.response_id
                            and response.nth_functional_group   = target_gs.nth_functional_group
            where       regexp_like(response.line_element_835, '^GS\\*.*')
            order by    1,2,3
        )
        , ge_trailer as
        (
            select      gs.response_id,
                        gs.nth_functional_group,
                        gs.index_ge                                                                                         as index,
                        concat_ws('*', gs.functional_group_trailer, target_gs.n_transaction_sets, gs.group_control_number)  as line_element_835
            from        edwprodhh.edi_835_parser.header_functional_group as gs
                        inner join
                            target_gs
                            on  gs.response_id          = target_gs.response_id
                            and gs.nth_functional_group = target_gs.nth_functional_group
                            
        )
        select      *
        from        gs_header
        union all
        select      *
        from        ge_trailer
    )
    , not_posted as
    (
        with target_gs as
        (
            select      response_id,
                        nth_functional_group,
                        count(distinct nth_transaction_set) as n_transaction_sets
            from        response_transaction_sets
            where       is_posted = 0
            group by    1,2
        )
        , gs_header as
        (
            select      response.response_id,
                        response.nth_functional_group,
                        response.index,
                        response.line_element_835
            from        response
                        inner join
                            target_gs
                            on  response.response_id            = target_gs.response_id
                            and response.nth_functional_group   = target_gs.nth_functional_group
            where       regexp_like(response.line_element_835, '^GS\\*.*')
            order by    1,2,3
        )
        , ge_trailer as
        (
            select      gs.response_id,
                        gs.nth_functional_group,
                        gs.index_ge                                                                                         as index,
                        concat_ws('*', gs.functional_group_trailer, target_gs.n_transaction_sets, gs.group_control_number)  as line_element_835
            from        edwprodhh.edi_835_parser.header_functional_group as gs
                        inner join
                            target_gs
                            on  gs.response_id          = target_gs.response_id
                            and gs.nth_functional_group = target_gs.nth_functional_group
                            
        )
        select      *
        from        gs_header
        union all
        select      *
        from        ge_trailer
    )
    select      *,
                1 as is_posted
    from        posted
    union all
    select      *,
                0 as is_posted
    from        not_posted
)
, response_interchange as
(
    with posted as
    (
        with target_isa as
        (
            select      response_id,
                        count(distinct nth_functional_group) as n_functional_groups
            from        response_functional_group
            where       is_posted = 1
            group by    1
        )
        , isa_header as
        (
            select      response.response_id,
                        response.index,
                        response.line_element_835
            from        response
                        inner join
                            target_isa
                            on response.response_id = target_isa.response_id
            where       regexp_like(response.line_element_835, '^ISA\\*.*')
        )
        , iea_trailer as
        (
            select      isa.response_id,
                        isa.index_iea                                                                                                           as index,
                        concat_ws('*', isa.interchange_control_trailer, target_isa.n_functional_groups, isa.interchange_control_number_iea)     as line_element_835
            from        edwprodhh.edi_835_parser.header_interchange_control as isa
                        inner join
                            target_isa
                            on isa.response_id = target_isa.response_id
        )
        select      *
        from        isa_header
        union all
        select      *
        from        iea_trailer
    )
    , not_posted as
    (
        with target_isa as
        (
            select      response_id,
                        count(distinct nth_functional_group) as n_functional_groups
            from        response_functional_group
            where       is_posted = 0
            group by    1
        )
        , isa_header as
        (
            select      response.response_id,
                        response.index,
                        response.line_element_835
            from        response
                        inner join
                            target_isa
                            on response.response_id = target_isa.response_id
            where       regexp_like(response.line_element_835, '^ISA\\*.*')
        )
        , iea_trailer as
        (
            select      isa.response_id,
                        isa.index_iea                                                                                                           as index,
                        concat_ws('*', isa.interchange_control_trailer, target_isa.n_functional_groups, isa.interchange_control_number_iea)     as line_element_835
            from        edwprodhh.edi_835_parser.header_interchange_control as isa
                        inner join
                            target_isa
                            on isa.response_id = target_isa.response_id
        )
        select      *
        from        isa_header
        union all
        select      *
        from        iea_trailer
    )
    select      *,
                1 as is_posted
    from        posted
    union all
    select      *,
                0 as is_posted
    from        not_posted
)
, unioned as
(
    select      response_id,
                index,
                line_element_835,
                is_posted
    from        response_interchange
    union all
    select      response_id,
                index,
                line_element_835,
                is_posted
    from        response_functional_group
    union all
    select      response_id,
                index,
                line_element_835,
                is_posted
    from        response_transaction_sets
)
select      response_id,
            index,
            line_element_835 || '~' as line_element_835,
            is_posted,
            current_date() as upload_date
from        unioned
order by    4,1,2,3
;


create or replace view
    edwprodhh.edi_835_parser.export_splicer_posted
as
select      *
from        edwprodhh.edi_835_parser.export_splicer_log
where       is_posted = 1
            and upload_date = current_date()
order by    1,2
;


create or replace view
    edwprodhh.edi_835_parser.export_splicer_unposted
as
select      *
from        edwprodhh.edi_835_parser.export_splicer_log
where       is_posted = 0
            and upload_date = current_date()
order by    1,2
;