create or replace table
    edwprodhh.edi_837_parser.hl_subscribers
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
    , subscriber_hl22 as
    (
        with filtered_hl as
        (
            select      *
            from        filtered
            where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                        and hl_index_subscriber_22  is not null
                        and hl_index_patient_23     is null
                        and claim_index             is null
        )
        , subscriber_hl as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'HL_PREFIX_SUBSCRIBER'
                                    when    flattened.index = 2   then      'HL_ID_SUBSCRIBER'
                                    when    flattened.index = 3   then      'HL_PARENT_ID_SUBSCRIBER'
                                    when    flattened.index = 4   then      'HL_LEVEL_CODE_SUBSCRIBER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                                    when    flattened.index = 5   then      'HL_CHILD_CODE_SUBSCRIBER' --1 HAS CHILD NODE, 0 NO CHILD NODE
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^HL.*')                          --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'HL_PREFIX_SUBSCRIBER',
                                'HL_ID_SUBSCRIBER',
                                'HL_PARENT_ID_SUBSCRIBER',
                                'HL_LEVEL_CODE_SUBSCRIBER',
                                'HL_CHILD_CODE_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            HL_PREFIX_SUBSCRIBER,
                            HL_ID_SUBSCRIBER,
                            HL_PARENT_ID_SUBSCRIBER,
                            HL_LEVEL_CODE_SUBSCRIBER,
                            HL_CHILD_CODE_SUBSCRIBER
                        )
        )
        , subscriber_sbr as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'SBR_PREFIX_SUBSCRIBER'
                                    when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER'     --P/S/T PRIMARY/SECONDARY/TERTIARY
                                    when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER'      --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                                    when    flattened.index = 4   then      'GROUP_NUMBER_SUBSCRIBER'
                                    when    flattened.index = 5   then      'GROUP_NAME_SUBSCRIBER'
                                    when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_SUBSCRIBER'               --12/13 PPO/HMO
                                    when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER'
                                    when    flattened.index = 8   then      'EMPLOYMENT_CODE_SUBSCRIBER'                   --F/P FULL/PART TIME
                                    when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER'       --CI/MB COMMERCIAL/MEDICARE PART B
                                    when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^SBR.*')                          --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'SBR_PREFIX_SUBSCRIBER',
                                'PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER',
                                'INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER',
                                'GROUP_NUMBER_SUBSCRIBER',
                                'GROUP_NAME_SUBSCRIBER',
                                'INSURANCE_TYPE_CODE_SUBSCRIBER',
                                'COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER',
                                'EMPLOYMENT_CODE_SUBSCRIBER',
                                'CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER',
                                'PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            SBR_PREFIX_SUBSCRIBEr,
                            PAYOR_RESPONSIBILITY_SEQUENCE_SUBSCRIBER,
                            INDIVIDUAL_RELATIONSHIP_CODE_SUBSCRIBER,
                            GROUP_NUMBER_SUBSCRIBER,
                            GROUP_NAME_SUBSCRIBER,
                            INSURANCE_TYPE_CODE_SUBSCRIBER,
                            COORDINATION_OF_BENEFITS_CODE_SUBSCRIBER,
                            EMPLOYMENT_CODE_SUBSCRIBER,
                            CLAIM_FILING_INDICATOR_CODE_SUBSCRIBER,
                            PATIENT_SIGNATURE_SOURCE_CODE_SUBSCRIBER
                        )
        )
        , subscriber_nmIL as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER'
                                    when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER'
                                    when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER'
                                    when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^NM1\\*IL.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_SUBSCRIBER',
                                'ENTITY_IDENTIFIER_CODE_SUBSCRIBER',
                                'ENTITY_TYPE_QUALIFIER_SUBSCRIBER',
                                'LAST_NAME_ORG_SUBSCRIBER',
                                'FIRST_NAME_SUBSCRIBER',
                                'MIDDLE_NAME_SUBSCRIBER',
                                'NAME_PREFIX_SUBSCRIBER',
                                'NAME_SUFFIX_SUBSCRIBER',
                                'ID_CODE_QUALIFIER_SUBSCRIBER',
                                'ID_CODE_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            NAME_CODE_SUBSCRIBER,
                            ENTITY_IDENTIFIER_CODE_SUBSCRIBER,
                            ENTITY_TYPE_QUALIFIER_SUBSCRIBER,
                            LAST_NAME_ORG_SUBSCRIBER,
                            FIRST_NAME_SUBSCRIBER,
                            MIDDLE_NAME_SUBSCRIBER,
                            NAME_PREFIX_SUBSCRIBER,
                            NAME_SUFFIX_SUBSCRIBER,
                            ID_CODE_QUALIFIER_SUBSCRIBER,
                            ID_CODE_SUBSCRIBER
                        )
        )
        , subscriber_n3 as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*IL'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_SUBSCRIBER',
                                'ADDRESS_LINE_1_SUBSCRIBER',
                                'ADDRESS_LINE_2_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_SUBSCRIBER,
                            ADDRESS_LINE_1_SUBSCRIBER,
                            ADDRESS_LINE_2_SUBSCRIBER
                        )
        )
        , subscriber_n4 as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER'
                                    when    flattened.index = 2   then      'CITY_SUBSCRIBER'
                                    when    flattened.index = 3   then      'ST_SUBSCRIBER'
                                    when    flattened.index = 4   then      'ZIP_SUBSCRIBER'
                                    when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*IL'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_SUBSCRIBER',
                                'CITY_SUBSCRIBER',
                                'ST_SUBSCRIBER',
                                'ZIP_SUBSCRIBER',
                                'COUNTRY_SUBSCRIBER',
                                'LOCATION_QUALIFIER_SUBSCRIBER',
                                'LOCATION_IDENTIFIER_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_SUBSCRIBER,
                            CITY_SUBSCRIBER,
                            ST_SUBSCRIBER,
                            ZIP_SUBSCRIBER,
                            COUNTRY_SUBSCRIBER,
                            LOCATION_QUALIFIER_SUBSCRIBER,
                            LOCATION_IDENTIFIER_SUBSCRIBER
                        )
        )
        , subscriber_dmg as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'DMG_PREFIX_SUBSCRIBER'
                                    when    flattened.index = 2   then      'FORMAT_QUALIFIER_SUBSCRIBER'
                                    when    flattened.index = 3   then      'DOB_SUBSCRIBER'
                                    when    flattened.index = 4   then      'GENDER_CODE_SUBSCRIBER'
                                    when    flattened.index = 5   then      'MARITAL_STATUS_SUBSCRIBER'
                                    when    flattened.index = 6   then      'ETHNICITY_CODE_SUBSCRIBER'
                                    when    flattened.index = 7   then      'CITIZENSHIP_CODE_SUBSCRIBER'
                                    when    flattened.index = 8   then      'COUNTRY_CODE_SUBSCRIBER'
                                    when    flattened.index = 9   then      'VERIFICATION_CODE_SUBSCRIBER'
                                    when    flattened.index = 10  then      'QUANTITY_SUBSCRIBER'
                                    when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_SUBSCRIBER'
                                    when    flattened.index = 12  then      'INDUSTRY_CODE_SUBSCRIBER'
                                    end     as value_header,

                            case    when    value_header = 'DOB_SUBSCRIBER'
                                    then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                                    else    nullif(trim(flattened.value), '')
                                    end     as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*IL'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'DMG_PREFIX_SUBSCRIBER',
                                'FORMAT_QUALIFIER_SUBSCRIBER',
                                'DOB_SUBSCRIBER',
                                'GENDER_CODE_SUBSCRIBER',
                                'MARITAL_STATUS_SUBSCRIBER',
                                'ETHNICITY_CODE_SUBSCRIBER',
                                'CITIZENSHIP_CODE_SUBSCRIBER',
                                'COUNTRY_CODE_SUBSCRIBER',
                                'VERIFICATION_CODE_SUBSCRIBER',
                                'QUANTITY_SUBSCRIBER',
                                'LIST_QUALIFIER_CODE_SUBSCRIBER',
                                'INDUSTRY_CODE_SUBSCRIBER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            DMG_PREFIX_SUBSCRIBER,
                            FORMAT_QUALIFIER_SUBSCRIBER,
                            DOB_SUBSCRIBER,
                            GENDER_CODE_SUBSCRIBER,
                            MARITAL_STATUS_SUBSCRIBER,
                            ETHNICITY_CODE_SUBSCRIBER,
                            CITIZENSHIP_CODE_SUBSCRIBER,
                            COUNTRY_CODE_SUBSCRIBER,
                            VERIFICATION_CODE_SUBSCRIBER,
                            QUANTITY_SUBSCRIBER,
                            LIST_QUALIFIER_CODE_SUBSCRIBER,
                            INDUSTRY_CODE_SUBSCRIBER
                        )
        )
        , subscriber_payor_nmPR as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 5   then      'FIRST_NAME_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 7   then      'NAME_PREFIX_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 10  then      'ID_CODE_SUBSCRIBER_PAYOR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^NM1\\*PR.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_SUBSCRIBER_PAYOR',
                                'ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR',
                                'ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR',
                                'LAST_NAME_ORG_SUBSCRIBER_PAYOR',
                                'FIRST_NAME_SUBSCRIBER_PAYOR',
                                'MIDDLE_NAME_SUBSCRIBER_PAYOR',
                                'NAME_PREFIX_SUBSCRIBER_PAYOR',
                                'NAME_SUFFIX_SUBSCRIBER_PAYOR',
                                'ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR',
                                'ID_CODE_SUBSCRIBER_PAYOR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            NAME_CODE_SUBSCRIBER_PAYOR,
                            ENTITY_IDENTIFIER_CODE_SUBSCRIBER_PAYOR,
                            ENTITY_TYPE_QUALIFIER_SUBSCRIBER_PAYOR,
                            LAST_NAME_ORG_SUBSCRIBER_PAYOR,
                            FIRST_NAME_SUBSCRIBER_PAYOR,
                            MIDDLE_NAME_SUBSCRIBER_PAYOR,
                            NAME_PREFIX_SUBSCRIBER_PAYOR,
                            NAME_SUFFIX_SUBSCRIBER_PAYOR,
                            ID_CODE_QUALIFIER_SUBSCRIBER_PAYOR,
                            ID_CODE_SUBSCRIBER_PAYOR
                        )
        )
        , subscriber_payor_n3 as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*PR'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                                'ADDRESS_LINE_1_SUBSCRIBER_PAYOR',
                                'ADDRESS_LINE_2_SUBSCRIBER_PAYOR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_SUBSCRIBER_PAYOR,
                            ADDRESS_LINE_1_SUBSCRIBER_PAYOR,
                            ADDRESS_LINE_2_SUBSCRIBER_PAYOR
                        )
        )
        , subscriber_payor_n4 as
        (
            with long as
            (
                select      filtered_hl.id_837,
                            filtered_hl.nth_transaction_set,
                            filtered_hl.index,
                            filtered_hl.hl_index_current,
                            filtered_hl.hl_index_billing_20,
                            filtered_hl.hl_index_subscriber_22,
                            filtered_hl.hl_index_patient_23,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 2   then      'CITY_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 3   then      'ST_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 4   then      'ZIP_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 5   then      'COUNTRY_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*PR'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_SUBSCRIBER_PAYOR',
                                'CITY_SUBSCRIBER_PAYOR',
                                'ST_SUBSCRIBER_PAYOR',
                                'ZIP_SUBSCRIBER_PAYOR',
                                'COUNTRY_SUBSCRIBER_PAYOR',
                                'LOCATION_QUALIFIER_SUBSCRIBER_PAYOR',
                                'LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_SUBSCRIBER_PAYOR,
                            CITY_SUBSCRIBER_PAYOR,
                            ST_SUBSCRIBER_PAYOR,
                            ZIP_SUBSCRIBER_PAYOR,
                            COUNTRY_SUBSCRIBER_PAYOR,
                            LOCATION_QUALIFIER_SUBSCRIBER_PAYOR,
                            LOCATION_IDENTIFIER_SUBSCRIBER_PAYOR
                        )
        )
        select      header.id_837,
                    header.nth_transaction_set,
                    header.index,
                    header.hl_index_current,
                    header.hl_index_billing_20,
                    header.hl_index_subscriber_22,
                    header.hl_index_patient_23,
                    header.hl_prefix_subscriber,
                    header.hl_id_subscriber,
                    header.hl_parent_id_subscriber,
                    header.hl_level_code_subscriber,
                    header.hl_child_code_subscriber,
                    sbr.sbr_prefix_subscriber,
                    sbr.payor_responsibility_sequence_subscriber,
                    sbr.individual_relationship_code_subscriber,
                    sbr.group_number_subscriber,
                    sbr.group_name_subscriber,
                    sbr.insurance_type_code_subscriber,
                    sbr.coordination_of_benefits_code_subscriber,
                    sbr.employment_code_subscriber,
                    sbr.claim_filing_indicator_code_subscriber,
                    sbr.patient_signature_source_code_subscriber,
                    nmIL.name_code_subscriber,
                    nmIL.entity_identifier_code_subscriber,
                    nmIL.entity_type_qualifier_subscriber,
                    nmIL.last_name_org_subscriber,
                    nmIL.first_name_subscriber,
                    nmIL.middle_name_subscriber,
                    nmIL.name_prefix_subscriber,
                    nmIL.name_suffix_subscriber,
                    nmIL.id_code_qualifier_subscriber,
                    nmIL.id_code_subscriber,
                    nmIL_n3.address_code_subscriber,
                    nmIL_n3.address_line_1_subscriber,
                    nmIL_n3.address_line_2_subscriber,
                    nmIL_n4.address_code_subscriber,
                    nmIL_n4.city_subscriber,
                    nmIL_n4.st_subscriber,
                    nmIL_n4.zip_subscriber,
                    nmIL_n4.country_subscriber,
                    nmIL_n4.location_qualifier_subscriber,
                    nmIL_n4.location_identifier_subscriber,
                    dmg.dmg_prefix_subscriber,
                    dmg.format_qualifier_subscriber,
                    dmg.dob_subscriber,
                    dmg.gender_code_subscriber,
                    dmg.marital_status_subscriber,
                    dmg.ethnicity_code_subscriber,
                    dmg.citizenship_code_subscriber,
                    dmg.country_code_subscriber,
                    dmg.verification_code_subscriber,
                    dmg.quantity_subscriber,
                    dmg.list_qualifier_code_subscriber,
                    dmg.industry_code_subscriber,
                    nmPR.name_code_subscriber_payor,
                    nmPR.entity_identifier_code_subscriber_payor,
                    nmPR.entity_type_qualifier_subscriber_payor,
                    nmPR.last_name_org_subscriber_payor,
                    nmPR.first_name_subscriber_payor,
                    nmPR.middle_name_subscriber_payor,
                    nmPR.name_prefix_subscriber_payor,
                    nmPR.name_suffix_subscriber_payor,
                    nmPR.id_code_qualifier_subscriber_payor,
                    nmPR.id_code_subscriber_payor,
                    nmPR_n3.address_code_subscriber_payor,
                    nmPR_n3.address_line_1_subscriber_payor,
                    nmPR_n3.address_line_2_subscriber_payor,
                    nmPR_n4.address_code_subscriber_payor,
                    nmPR_n4.city_subscriber_payor,
                    nmPR_n4.st_subscriber_payor,
                    nmPR_n4.zip_subscriber_payor,
                    nmPR_n4.country_subscriber_payor,
                    nmPR_n4.location_qualifier_subscriber_payor,
                    nmPR_n4.location_identifier_subscriber_payor

        from        subscriber_hl           as header
                    left join
                        subscriber_sbr          as sbr
                        on  header.id_837                   = sbr.id_837
                        and header.nth_transaction_set      = sbr.nth_transaction_set
                        and header.hl_index_subscriber_22   = sbr.hl_index_subscriber_22
                    left join
                        subscriber_nmIL         as nmIL
                        on  header.id_837                   = nmIL.id_837
                        and header.nth_transaction_set      = nmIL.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmIL.hl_index_subscriber_22
                    left join
                        subscriber_n3           as nmIL_n3
                        on  header.id_837                   = nmIL_n3.id_837
                        and header.nth_transaction_set      = nmIL_n3.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmIL_n3.hl_index_subscriber_22
                    left join
                        subscriber_n4           as nmIL_n4
                        on  header.id_837                   = nmIL_n4.id_837
                        and header.nth_transaction_set      = nmIL_n4.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmIL_n4.hl_index_subscriber_22
                    left join
                        subscriber_dmg          as dmg
                        on  header.id_837                   = dmg.id_837
                        and header.nth_transaction_set      = dmg.nth_transaction_set
                        and header.hl_index_subscriber_22   = dmg.hl_index_subscriber_22
                    left join
                        subscriber_payor_nmPR   as nmPR
                        on  header.id_837                   = nmPR.id_837
                        and header.nth_transaction_set      = nmPR.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmPR.hl_index_subscriber_22
                    left join
                        subscriber_payor_n3     as nmPR_n3
                        on  header.id_837                   = nmPR_n3.id_837
                        and header.nth_transaction_set      = nmPR_n3.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmPR_n3.hl_index_subscriber_22
                    left join
                        subscriber_payor_n4     as nmPR_n4
                        on  header.id_837                   = nmPR_n4.id_837
                        and header.nth_transaction_set      = nmPR_n4.nth_transaction_set
                        and header.hl_index_subscriber_22   = nmPR_n4.hl_index_subscriber_22
    )
    select      *
    from        subscriber_hl22
)
select      *
from        parse_transaction_sets
;