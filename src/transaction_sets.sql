create or replace table
    edwprodhh.edi_837_parser.transaction_sets
as
with sample_837_ as
(
    with raw_837 as
    (
        select      1 as id_837,
        'ISA*00*          *00*          *ZZ*580977458      *ZZ*12345678       *250427*0130*^*00501*000000001*1*P*:~
        GS*HC*580977458*12345678*20250427*013049*000000001*X*005010X223A2~
        ST*837*000000526*005010X223A2~
        BHT*0019*00*601160001*20250426*080727*CH~
        NM1*41*2*INDIANA UNIVERSITY HEALTH*****46*7797~
        PER*IC*CLEARINGHOUSE PRODUCTION*TE*8005278133*FX*9184814275*EM*ASSURANCESUPPORT@CHANGEHEALTHCARE.COM~
        NM1*40*2*39026 UHC GEHA*****46*12345678~
        HL*1**20*1~
        PRV*BI*PXC*282N00000X~
        NM1*85*2*INDIANA UNIVERSITY HEALTH*****XX*1568492916~
        N3*11700 N MERIDIAN ST~
        N4*CARMEL*IN*460324656~
        REF*EI*351932442~
        PER*IC*INDIANA UNIVERSITY HEALTH*TE*3179628661~
        NM1*87*2~
        N3*8227 RELIABLE PARKWAY~
        N4*CHICAGO*IL*606868227~
        HL*2*1*22*1~
        SBR*P**76416933******12~
        NM1*IL*1*EGGINK*ALFONS*G***MI*G44857197~
        NM1*PR*2*39026 UHC GEHA*****PI*4590~
        HL*3*2*23*0~
        PAT*01~
        NM1*QC*1*BAUMER*ALLISON~
        N3*6293 VALLEYVIEW DR~
        N4*FISHERS*IN*460382083~
        DMG*D8*19890711*F~
        CLM*1269183370*6407***13:A:1**A*Y*Y~
        DTP*434*RD8*20250419-20250419~
        CL1*1*1*01~
        REF*D9*2511610517977~
        REF*EA*75039746~
        HI*ABK:O368130~
        HI*APR:O26893~
        HI*ABF:O4693*ABF:O99343*ABF:O99283*ABF:E039*ABF:F419*ABF:Z3A33~
        HI*BH:11:D8:20250419~
        NM1*71*1*MATHAI*MICAH****XX*1407313489~
        LX*1~
        SV2*0306*HC:87150*191*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003001~
        LX*2~
        SV2*0306*HC:87480*110*UN*1~sbr
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003002~
        LX*3~
        SV2*0306*HC:87510*110*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003003~
        LX*4~
        SV2*0306*HC:87653*191*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003004~
        LX*5~
        SV2*0306*HC:87660*110*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003005~
        LX*6~
        SV2*0450*HC:99284:25*4012*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003006~
        LX*7~
        SV2*0920*HC:59025*1683*UN*1~
        DTP*472*D8*20250419~
        REF*6R*00126918337000Q98003007~
        SE*64*000000526~
        ST*837*000000527*005010X223A2~
        BHT*0019*00*601160001*20250426*080724*CH~
        NM1*41*2*IU HEALTH PAOLI*****46*7797~
        PER*IC*CLEARINGHOUSE PRODUCTION*TE*8005278133*FX*9184814275*EM*ASSURANCESUPPORT@CHANGEHEALTHCARE.COM~
        NM1*40*2*ANTHEM HMO MEDICARE*****46*12345678~
        HL*1**20*1~
        PRV*BI*PXC*282NC0060X~
        NM1*85*2*IU HEALTH PAOLI*****XX*1912984451~
        N3*642 W HOSPITAL RD~
        N4*PAOLI*IN*474549672~
        REF*EI*352090919~
        PER*IC*IU HEALTH PAOLI*TE*3179628661~
        NM1*87*2~
        N3*7992 SOLUTION CENTER~
        N4*CHICAGO*IL*606777009~
        HL*2*1*22*0~
        SBR*P*18*INMCRWP0******BL~
        NM1*IL*1*ALSPAUGH*CLASKA****MI*VOK998W03022~
        N3*11390 W STATE ROAD 56~
        N4*FRENCH LICK*IN*474327904~
        DMG*D8*19440727*F~
        NM1*PR*2*ANTHEM HMO MEDICARE*****PI*3502~
        CLM*1270351230*18***85:A:1**A*Y*Y~
        DTP*434*RD8*20250308-20250308~
        CL1*3*1*01~
        REF*D9*2511610517900~
        REF*EA*74323983~
        HI*ABK:R109~
        NM1*71*1*SLONE*TERESA****XX*1912359860~
        LX*1~
        SV2*0307*HC:81001*18*UN*1~
        DTP*472*D8*20250308~
        REF*6R*00127035123000M28003001~
        SE*34*000000527~'
         as sample_837
    )
    select      id_837,
                regexp_replace(sample_837, '~$', '') as sample_837
    from        raw_837
)
, flatten_837 as
(
    with flattened_ as
    (
        select      id_837,
                    index,
                    trim(regexp_replace(value, '\\s+', ' ')) as line_element_837
        from        sample_837_,
                    lateral split_to_table(sample_837_.sample_837, '~') as flattened
    )
    select      *,
                count_if(regexp_like(line_element_837, '^ST.*')) over (partition by id_837 order by index asc) as nth_transaction_set
    from        flattened_
)
, parse_transaction_sets as
(
    with filtered as
    (
        with add_hl_index as
        (
            select      *,
                        max(case when regexp_like(line_element_837, '^HL.*')                then index end) over (partition by id_837, nth_transaction_set order by index asc) as hl_index_current,
                        max(case when regexp_like(line_element_837, '^HL.*20\\*[^\\*]*$')   then index end) over (partition by id_837, nth_transaction_set order by index asc) as hl_index_billing_20,
                        max(case when regexp_like(line_element_837, '^HL.*22\\*[^\\*]*$')   then index end) over (partition by id_837, nth_transaction_set order by index asc) as hl_index_subscriber_22,
                        max(case when regexp_like(line_element_837, '^HL.*23\\*[^\\*]*$')   then index end) over (partition by id_837, nth_transaction_set order by index asc) as hl_index_patient_23,
                        
            from        flatten_837
            where       nth_transaction_set != 0
                        -- and nth_transaction_set = 1 --temporary for testing
        )
        , add_clm_index as
        (
            select      *,
                        max(case when regexp_like(line_element_837, '^CLM.*')               then index end)                                                             over (partition by id_837, nth_transaction_set, hl_index_current order by index asc)    as claim_index,
                        coalesce(
                            regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)'),
                            lag(case when regexp_like(line_element_837, '^NM1.*')           then regexp_substr(line_element_837, '^(NM1\\*[^\\*]*)') end) ignore nulls  over (partition by id_837, nth_transaction_set, hl_index_current order by index asc)
                        )                                                                                                                                                                                                                                       as lag_name_indicator
            from        add_hl_index
        )
        , add_lx_index as
        (
            select      *,
                        max(case when regexp_like(line_element_837, '^LX.*')                then index end) over (partition by id_837, nth_transaction_set, claim_index order by index asc) as lx_index,
                        max(case when regexp_like(line_element_837, '^SBR.*')               then index end) over (partition by id_837, nth_transaction_set, claim_index order by index asc) as other_sbr_index
            from        add_clm_index
        )
        select      *
        from        add_lx_index
    )
    , header_st as
    (
        with long as
        (
            select      filtered.id_837,
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
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^ST.*')                         --1 Filter
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
                        ID_837,
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
            select      filtered.id_837,
                        filtered.nth_transaction_set,

                        -- flattened.index,
                        -- nullif(trim(flattened.value), '') as value_raw,

                        case    when    flattened.index = 1   then      'TRANSACTION_SET_TRAILER'
                                when    flattened.index = 2   then      'TRANSACTION_SEGMENT_COUNT'
                                when    flattened.index = 3   then      'TRANSACTION_SET_CONTROL_NUMBER_TRAILER'
                                end     as value_header,

                        nullif(trim(flattened.value), '') as value_format

            from        filtered,
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^SE.*')                         --1 Filter
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
                        ID_837,
                        NTH_TRANSACTION_SET,
                        TRANSACTION_SET_TRAILER,
                        TRANSACTION_SEGMENT_COUNT,
                        TRANSACTION_SET_CONTROL_NUMBER_TRAILER
                    )
    )
    , beginning_bht as
    (
        with long as
        (
            select      filtered.id_837,
                        filtered.nth_transaction_set,

                        -- flattened.index,
                        -- nullif(trim(flattened.value), '') as value_raw,

                        case    when    flattened.index = 1   then      'BEGINNING_OF_HIERARCHICAL_TRANSACTION'
                                when    flattened.index = 2   then      'HIERARCHICAL_STRUCTURE_CODE'
                                when    flattened.index = 3   then      'TRANSACTION_SET_PURPOSE_CODE'
                                when    flattened.index = 4   then      'ORIGINATOR_APPLICATION_TRANSACTION_ID'
                                when    flattened.index = 5   then      'TRANSACTION_SET_CREATED_DATE'
                                when    flattened.index = 6   then      'TRANSACTION_SET_CREATED_TIME'
                                when    flattened.index = 7   then      'TRANSACTION_TYPE_CODE'
                                end     as value_header,

                        case    when    value_header = 'TRANSACTION_SET_CREATED_DATE'  then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                                when    value_header = 'TRANSACTION_SET_CREATED_TIME'  then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                                else    nullif(trim(flattened.value), '')
                                end     as value_format

            from        filtered,
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^BHT.*')                         --1 Filter
        )
        select      *,
                    (TRANSACTION_SET_CREATED_DATE || ' ' || TRANSACTION_SET_CREATED_TIME)::timestamp as TRANSACTION_SET_CREATED_timestamp
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'BEGINNING_OF_HIERARCHICAL_TRANSACTION',
                            'HIERARCHICAL_STRUCTURE_CODE',
                            'TRANSACTION_SET_PURPOSE_CODE',
                            'ORIGINATOR_APPLICATION_TRANSACTION_ID',
                            'TRANSACTION_SET_CREATED_DATE',
                            'TRANSACTION_SET_CREATED_TIME',
                            'TRANSACTION_TYPE_CODE'
                        )
                    )   as pvt (
                        ID_837,
                        NTH_TRANSACTION_SET,
                        BEGINNING_OF_HIERARCHICAL_TRANSACTION,
                        HIERARCHICAL_STRUCTURE_CODE,
                        TRANSACTION_SET_PURPOSE_CODE,
                        ORIGINATOR_APPLICATION_TRANSACTION_ID,
                        TRANSACTION_SET_CREATED_DATE,
                        TRANSACTION_SET_CREATED_TIME,
                        TRANSACTION_TYPE_CODE
                    )
    )
    , submitter_nm41 as
    (
        with long as
        (
            select      filtered.id_837,
                        filtered.nth_transaction_set,

                        -- flattened.index,
                        -- nullif(trim(flattened.value), '') as value_raw,

                        case    when    flattened.index = 1   then      'NAME_CODE_SUBMITTER'
                                when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBMITTER'
                                when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBMITTER'
                                when    flattened.index = 4   then      'LAST_NAME_ORG_SUBMITTER'
                                when    flattened.index = 5   then      'FIRST_NAME_SUBMITTER'
                                when    flattened.index = 6   then      'MIDDLE_NAME_SUBMITTER'
                                when    flattened.index = 7   then      'NAME_PREFIX_SUBMITTER'
                                when    flattened.index = 8   then      'NAME_SUFFIX_SUBMITTER'
                                when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBMITTER'
                                when    flattened.index = 10  then      'ID_CODE_SUBMITTER'
                                end     as value_header,

                        nullif(trim(flattened.value), '') as value_format

            from        filtered,
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^NM1\\*41.*')                         --1 Filter
        )
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'NAME_CODE_SUBMITTER',
                            'ENTITY_IDENTIFIER_CODE_SUBMITTER',
                            'ENTITY_TYPE_QUALIFIER_SUBMITTER',
                            'LAST_NAME_ORG_SUBMITTER',
                            'FIRST_NAME_SUBMITTER',
                            'MIDDLE_NAME_SUBMITTER',
                            'NAME_PREFIX_SUBMITTER',
                            'NAME_SUFFIX_SUBMITTER',
                            'ID_CODE_QUALIFIER_SUBMITTER',
                            'ID_CODE_SUBMITTER'
                        )
                    )   as pvt (
                        ID_837,
                        NTH_TRANSACTION_SET,
                        NAME_CODE_SUBMITTER,
                        ENTITY_IDENTIFIER_CODE_SUBMITTER,
                        ENTITY_TYPE_QUALIFIER_SUBMITTER,
                        LAST_NAME_ORG_SUBMITTER,
                        FIRST_NAME_SUBMITTER,
                        MIDDLE_NAME_SUBMITTER,
                        NAME_PREFIX_SUBMITTER,
                        NAME_SUFFIX_SUBMITTER,
                        ID_CODE_QUALIFIER_SUBMITTER,
                        ID_CODE_SUBMITTER
                    )
    )
    , submitter_nm41_per as
    (
        with long as
        (
            select      filtered.id_837,
                        filtered.nth_transaction_set,

                        -- flattened.index,
                        -- nullif(trim(flattened.value), '') as value_raw,

                        case    when    flattened.index = 1   then      'SUBMITTER_CONTACT_PREFIX'
                                when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE'
                                when    flattened.index = 3   then      'SUBMITTER_CONTACT_NAME'
                                when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1'
                                when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1'
                                when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2'
                                when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2'
                                when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3'
                                when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3'
                                end     as value_header,

                        nullif(trim(flattened.value), '') as value_format

            from        filtered,
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^PER.*')                         --1 Filter
                        and filtered.lag_name_indicator = 'NM1*41'
        )
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'SUBMITTER_CONTACT_PREFIX',
                            'CONTACT_FUNCTION_CODE',
                            'SUBMITTER_CONTACT_NAME',
                            'COMMUNICATION_QUALIFIER_1',
                            'COMMUNICATION_NUMBER_1',
                            'COMMUNICATION_QUALIFIER_2',
                            'COMMUNICATION_NUMBER_2',
                            'COMMUNICATION_QUALIFIER_3',
                            'COMMUNICATION_NUMBER_3'
                        )
                    )   as pvt (
                        ID_837,
                        NTH_TRANSACTION_SET,
                        SUBMITTER_CONTACT_PREFIX,
                        CONTACT_FUNCTION_CODE,
                        SUBMITTER_CONTACT_NAME,
                        COMMUNICATION_QUALIFIER_1,
                        COMMUNICATION_NUMBER_1,
                        COMMUNICATION_QUALIFIER_2,
                        COMMUNICATION_NUMBER_2,
                        COMMUNICATION_QUALIFIER_3,
                        COMMUNICATION_NUMBER_3
                    )
    )
    , receiver_nm40 as
    (
        with long as
        (
            select      filtered.id_837,
                        filtered.nth_transaction_set,

                        -- flattened.index,
                        -- nullif(trim(flattened.value), '') as value_raw,

                        case    when    flattened.index = 1   then      'NAME_CODE_RECEIVER'
                                when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_RECEIVER'
                                when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_RECEIVER'
                                when    flattened.index = 4   then      'LAST_NAME_ORG_RECEIVER'
                                when    flattened.index = 5   then      'FIRST_NAME_RECEIVER'
                                when    flattened.index = 6   then      'MIDDLE_NAME_RECEIVER'
                                when    flattened.index = 7   then      'NAME_PREFIX_RECEIVER'
                                when    flattened.index = 8   then      'NAME_SUFFIX_RECEIVER'
                                when    flattened.index = 9   then      'ID_CODE_QUALIFIER_RECEIVER'
                                when    flattened.index = 10  then      'ID_CODE_RECEIVER'
                                end     as value_header,

                        nullif(trim(flattened.value), '') as value_format

            from        filtered,
                        lateral split_to_table(filtered.line_element_837, '*') as flattened     --2 Flatten

            where       regexp_like(filtered.line_element_837, '^NM1\\*40.*')                   --1 Filter
        )
        select      *
        from        long
                    pivot(
                        max(value_format) for value_header in (
                            'NAME_CODE_RECEIVER',
                            'ENTITY_IDENTIFIER_CODE_RECEIVER',
                            'ENTITY_TYPE_QUALIFIER_RECEIVER',
                            'LAST_NAME_ORG_RECEIVER',
                            'FIRST_NAME_RECEIVER',
                            'MIDDLE_NAME_RECEIVER',
                            'NAME_PREFIX_RECEIVER',
                            'NAME_SUFFIX_RECEIVER',
                            'ID_CODE_QUALIFIER_RECEIVER',
                            'ID_CODE_RECEIVER'
                        )
                    )   as pvt (
                        ID_837,
                        NTH_TRANSACTION_SET,
                        NAME_CODE_RECEIVER,
                        ENTITY_IDENTIFIER_CODE_RECEIVER,
                        ENTITY_TYPE_QUALIFIER_RECEIVER,
                        LAST_NAME_ORG_RECEIVER,
                        FIRST_NAME_RECEIVER,
                        MIDDLE_NAME_RECEIVER,
                        NAME_PREFIX_RECEIVER,
                        NAME_SUFFIX_RECEIVER,
                        ID_CODE_QUALIFIER_RECEIVER,
                        ID_CODE_RECEIVER
                    )
    )
    select      header.id_837,
                header.nth_transaction_set,
                header.transaction_set_header,
                header.transaction_set_id_code,
                header.transaction_set_control_number_header,
                header.implementation_convention_reference,
                trailer.transaction_set_trailer,
                trailer.transaction_segment_count,
                trailer.transaction_set_control_number_trailer,
                bht.beginning_of_hierarchical_transaction,
                bht.hierarchical_structure_code,
                bht.transaction_set_purpose_code,
                bht.originator_application_transaction_id,
                bht.transaction_set_created_date,
                bht.transaction_set_created_time,
                bht.transaction_type_code,
                nm41.name_code_submitter,
                nm41.entity_identifier_code_submitter,
                nm41.entity_type_qualifier_submitter,
                nm41.last_name_org_submitter,
                nm41.first_name_submitter,
                nm41.middle_name_submitter,
                nm41.name_prefix_submitter,
                nm41.name_suffix_submitter,
                nm41.id_code_qualifier_submitter,
                nm41.id_code_submitter,
                nm41_per.submitter_contact_prefix,
                nm41_per.contact_function_code,
                nm41_per.submitter_contact_name,
                nm41_per.communication_qualifier_1,
                nm41_per.communication_number_1,
                nm41_per.communication_qualifier_2,
                nm41_per.communication_number_2,
                nm41_per.communication_qualifier_3,
                nm41_per.communication_number_3,
                nm40.name_code_receiver,
                nm40.entity_identifier_code_receiver,
                nm40.entity_type_qualifier_receiver,
                nm40.last_name_org_receiver,
                nm40.first_name_receiver,
                nm40.middle_name_receiver,
                nm40.name_prefix_receiver,
                nm40.name_suffix_receiver,
                nm40.id_code_qualifier_receiver,
                nm40.id_code_receiver

    from        header_st               as header
                left join
                    trailer_se          as trailer
                    on  header.id_837                   = trailer.id_837
                    and header.nth_transaction_set      = trailer.nth_transaction_set
                left join
                    beginning_bht       as bht
                    on  header.id_837                   = bht.id_837
                    and header.nth_transaction_set      = bht.nth_transaction_set
                left join
                    submitter_nm41      as nm41
                    on  header.id_837                   = nm41.id_837
                    and header.nth_transaction_set      = nm41.nth_transaction_set
                left join
                    submitter_nm41_per  as nm41_per
                    on  header.id_837                   = nm41_per.id_837
                    and header.nth_transaction_set      = nm41_per.nth_transaction_set
                left join
                    receiver_nm40       as nm40
                    on  header.id_837                   = nm40.id_837
                    and header.nth_transaction_set      = nm40.nth_transaction_set
)
select      *
from        parse_transaction_sets
;