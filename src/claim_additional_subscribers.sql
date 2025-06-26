create or replace table
    edwprodhh.edi_837_parser.claim_additional_subscribers
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
    , clm_othersbr as
    (
        with filtered_clm_sbr as
        (
            select      *
            from        filtered
            where       claim_index is not null --0 Pre-Filter
                        and other_sbr_index is not null
                        and lx_index is null
        )
        , claim_sbr_header as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'SBR_PREFIX_OTHERSBR'
                                    when    flattened.index = 2   then      'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR'
                                    when    flattened.index = 3   then      'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR'
                                    when    flattened.index = 4   then      'GROUP_NUMBER_OTHERSBR'
                                    when    flattened.index = 5   then      'GROUP_NAME_OTHERSBR'
                                    when    flattened.index = 6   then      'INSURANCE_TYPE_CODE_OTHERSBR'
                                    when    flattened.index = 7   then      'COORDINATION_OF_BENEFITS_CODE_OTHERSBR'
                                    when    flattened.index = 8   then      'EMPLOYMENT_CODE_OTHERSBR'
                                    when    flattened.index = 9   then      'CLAIM_FILING_INDICATOR_CODE_OTHERSBR'
                                    when    flattened.index = 10  then      'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^SBR.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'SBR_PREFIX_OTHERSBR',
                                'PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR',
                                'INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR',
                                'GROUP_NUMBER_OTHERSBR',
                                'GROUP_NAME_OTHERSBR',
                                'INSURANCE_TYPE_CODE_OTHERSBR',
                                'COORDINATION_OF_BENEFITS_CODE_OTHERSBR',
                                'EMPLOYMENT_CODE_OTHERSBR',
                                'CLAIM_FILING_INDICATOR_CODE_OTHERSBR',
                                'PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            SBR_PREFIX_OTHERSBR,
                            PAYOR_RESPONSIBILITY_SEQUENCE_OTHERSBR,
                            INDIVIDUAL_RELATIONSHIP_CODE_OTHERSBR,
                            GROUP_NUMBER_OTHERSBR,
                            GROUP_NAME_OTHERSBR,
                            INSURANCE_TYPE_CODE_OTHERSBR,
                            COORDINATION_OF_BENEFITS_CODE_OTHERSBR,
                            EMPLOYMENT_CODE_OTHERSBR,
                            CLAIM_FILING_INDICATOR_CODE_OTHERSBR,
                            PATIENT_SIGNATURE_SOURCE_CODE_OTHERSBR
                        )
        )
        , claim_sbr_cas as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1     then      'PREFIX_CAS'
                                    when    flattened.index = 2     then      'CLM_ADJ_GROUP_CODE'
                                    when    flattened.index = 3     then      'ADJ_REASON_CODE_1'
                                    when    flattened.index = 4     then      'ADJ_AMOUNT_1'
                                    when    flattened.index = 5     then      'ADJ_QUANTITY_1'
                                    when    flattened.index = 6     then      'ADJ_REASON_CODE_2'
                                    when    flattened.index = 7     then      'ADJ_AMOUNT_2'
                                    when    flattened.index = 8     then      'ADJ_QUANTITY_2'
                                    when    flattened.index = 9     then      'ADJ_REASON_CODE_3'
                                    when    flattened.index = 10    then      'ADJ_AMOUNT_3'
                                    when    flattened.index = 11    then      'ADJ_QUANTITY_3'
                                    when    flattened.index = 12    then      'ADJ_REASON_CODE_4'
                                    when    flattened.index = 13    then      'ADJ_AMOUNT_4'
                                    when    flattened.index = 14    then      'ADJ_QUANTITY_4'
                                    when    flattened.index = 15    then      'ADJ_REASON_CODE_5'
                                    when    flattened.index = 16    then      'ADJ_AMOUNT_5'
                                    when    flattened.index = 17    then      'ADJ_QUANTITY_5'
                                    when    flattened.index = 18    then      'ADJ_REASON_CODE_6'
                                    when    flattened.index = 19    then      'ADJ_AMOUNT_6'
                                    when    flattened.index = 20    then      'ADJ_QUANTITY_6'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^CAS.*')                         --1 Filter
            )
            , pivoted as
            (
                select      *
                from        long
                            pivot(
                                max(value_format) for value_header in (
                                    'PREFIX_CAS',
                                    'CLM_ADJ_GROUP_CODE',
                                    'ADJ_REASON_CODE_1',
                                    'ADJ_AMOUNT_1',
                                    'ADJ_QUANTITY_1',
                                    'ADJ_REASON_CODE_2',
                                    'ADJ_AMOUNT_2',
                                    'ADJ_QUANTITY_2',
                                    'ADJ_REASON_CODE_3',
                                    'ADJ_AMOUNT_3',
                                    'ADJ_QUANTITY_3',
                                    'ADJ_REASON_CODE_4',
                                    'ADJ_AMOUNT_4',
                                    'ADJ_QUANTITY_4',
                                    'ADJ_REASON_CODE_5',
                                    'ADJ_AMOUNT_5',
                                    'ADJ_QUANTITY_5',
                                    'ADJ_REASON_CODE_6',
                                    'ADJ_AMOUNT_6',
                                    'ADJ_QUANTITY_6'
                                )
                            )   as pvt (
                                ID_837,
                                NTH_TRANSACTION_SET,
                                INDEX,
                                HL_INDEX_CURRENT,
                                HL_INDEX_BILLING_20,
                                HL_INDEX_SUBSCRIBER_22,
                                HL_INDEX_PATIENT_23,
                                CLAIM_INDEX,
                                OTHER_SBR_INDEX,
                                PREFIX_CAS,
                                CLM_ADJ_GROUP_CODE,
                                ADJ_REASON_CODE_1,
                                ADJ_AMOUNT_1,
                                ADJ_QUANTITY_1,
                                ADJ_REASON_CODE_2,
                                ADJ_AMOUNT_2,
                                ADJ_QUANTITY_2,
                                ADJ_REASON_CODE_3,
                                ADJ_AMOUNT_3,
                                ADJ_QUANTITY_3,
                                ADJ_REASON_CODE_4,
                                ADJ_AMOUNT_4,
                                ADJ_QUANTITY_4,
                                ADJ_REASON_CODE_5,
                                ADJ_AMOUNT_5,
                                ADJ_QUANTITY_5,
                                ADJ_REASON_CODE_6,
                                ADJ_AMOUNT_6,
                                ADJ_QUANTITY_6
                            )
            )
            , unpivoted as
            (
                select      id_837,
                            nth_transaction_set,
                            index,
                            hl_index_current,
                            hl_index_billing_20,
                            hl_index_subscriber_22,
                            hl_index_patient_23,
                            claim_index,
                            other_sbr_index,
                            clm_adj_group_code,
                            regexp_substr(unpvt.metric_name, '\\d+$') as nth_element,
                            regexp_replace(unpvt.metric_name, '_\\d+$', '') as metric_name,
                            metric_value
                from        pivoted
                            unpivot include nulls (
                                metric_value for metric_name in (
                                    ADJ_REASON_CODE_1,
                                    ADJ_AMOUNT_1,
                                    ADJ_QUANTITY_1,
                                    ADJ_REASON_CODE_2,
                                    ADJ_AMOUNT_2,
                                    ADJ_QUANTITY_2,
                                    ADJ_REASON_CODE_3,
                                    ADJ_AMOUNT_3,
                                    ADJ_QUANTITY_3,
                                    ADJ_REASON_CODE_4,
                                    ADJ_AMOUNT_4,
                                    ADJ_QUANTITY_4,
                                    ADJ_REASON_CODE_5,
                                    ADJ_AMOUNT_5,
                                    ADJ_QUANTITY_5,
                                    ADJ_REASON_CODE_6,
                                    ADJ_AMOUNT_6,
                                    ADJ_QUANTITY_6
                                )
                            )   as unpvt
            )
            select      id_837,
                        nth_transaction_set,
                        claim_index,
                        other_sbr_index,
                        array_agg(
                            object_construct_keep_null(
                                'adj_group_code',   clm_adj_group_code,
                                'adj_detail',       object_construct_keep_null(
                                                        'adj_reason_code',  adj_reason_code::varchar,
                                                        'adj_amount',       adj_amount::number(18,2),
                                                        'adj_quantity',     adj_quantity::number(18,2)
                                                    )
                            )
                        )   as cas_adj_array
            from        unpivoted
                        pivot (
                            max(metric_value) for metric_name in (
                                'ADJ_REASON_CODE',
                                'ADJ_AMOUNT',
                                'ADJ_QUANTITY'
                            )
                        )   as pvt (
                            id_837,
                            nth_transaction_set,
                            index,
                            hl_index_current,
                            hl_index_billing_20,
                            hl_index_subscriber_22,
                            hl_index_patient_23,
                            claim_index,
                            other_sbr_index,
                            clm_adj_group_code,
                            nth_element,
                            adj_reason_code,
                            adj_amount,
                            adj_quantity
                        )
            where       not (
                            adj_reason_code is null
                            and adj_amount is null
                            and adj_quantity is null
                        )
            group by    1,2,3,4
            order by    1,2,3,4
        )
        , claim_sbr_amt as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1     then      'PREFIX_AMT'
                                    when    flattened.index = 2     then      'AMT_QUALIFIER_CODE'
                                    when    flattened.index = 3     then      'MONETARY_AMOUNT'
                                    when    flattened.index = 4     then      'CREDIT_DEBIT_FLAG'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^AMT.*')                         --1 Filter
            )
            , pivoted as
            (
                select      *
                from        long
                            pivot(
                                max(value_format) for value_header in (
                                    'PREFIX_AMT',
                                    'AMT_QUALIFIER_CODE',
                                    'MONETARY_AMOUNT',
                                    'CREDIT_DEBIT_FLAG'
                                )
                            )   as pvt (
                                ID_837,
                                NTH_TRANSACTION_SET,
                                INDEX,
                                HL_INDEX_CURRENT,
                                HL_INDEX_BILLING_20,
                                HL_INDEX_SUBSCRIBER_22,
                                HL_INDEX_PATIENT_23,
                                CLAIM_INDEX,
                                OTHER_SBR_INDEX,
                                PREFIX_AMT,
                                AMT_QUALIFIER_CODE,
                                MONETARY_AMOUNT,
                                CREDIT_DEBIT_FLAG
                            )
            )
            select      id_837,
                        nth_transaction_set,
                        claim_index,
                        other_sbr_index,
                        array_agg(
                            object_construct_keep_null(
                                'amt_qualifier_code',   amt_qualifier_code::varchar,
                                'monetary_amount',      monetary_amount::number(18,2),
                                'credit_debit_flag',    credit_debit_flag::varchar
                            )
                        )   as amt_adj_array
            from        pivoted
            group by    1,2,3,4
            order by    1,2,3,4
        )
        , claim_sbr_oi as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'PREFIX_OTHERSBR'
                                    when    flattened.index = 2   then      'EMPTY1_OTHERSBR'
                                    when    flattened.index = 3   then      'EMPTY2_OTHERSBR'
                                    when    flattened.index = 4   then      'BENEFITS_ASSIGNMENT_OTHERSBR'
                                    when    flattened.index = 5   then      'PATIENT_SIGNATURE_SOURCE_OTHERSBR'
                                    when    flattened.index = 6   then      'EMPTY5_OTHERSBR'
                                    when    flattened.index = 7   then      'RELEASE_OF_INFO_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^OI.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PREFIX_OTHERSBR',
                                'EMPTY1_OTHERSBR',
                                'EMPTY2_OTHERSBR',
                                'BENEFITS_ASSIGNMENT_OTHERSBR',
                                'PATIENT_SIGNATURE_SOURCE_OTHERSBR',
                                'EMPTY5_OTHERSBR',
                                'RELEASE_OF_INFO_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            PREFIX_OTHERSBR,
                            EMPTY1_OTHERSBR,
                            EMPTY2_OTHERSBR,
                            BENEFITS_ASSIGNMENT_OTHERSBR,
                            PATIENT_SIGNATURE_SOURCE_OTHERSBR,
                            EMPTY5_OTHERSBR,
                            RELEASE_OF_INFO_OTHERSBR
                        )
        )
        , claim_sbr_nmIL as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_OTHERSBR'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERSBR'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERSBR'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERSBR'
                                    when    flattened.index = 5   then      'FIRST_NAME_OTHERSBR'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_OTHERSBR'
                                    when    flattened.index = 7   then      'NAME_PREFIX_OTHERSBR'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_OTHERSBR'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERSBR'
                                    when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*IL.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_OTHERSBR',
                                'ENTITY_IDENTIFIER_CODE_OTHERSBR',
                                'ENTITY_TYPE_QUALIFIER_OTHERSBR',
                                'LAST_NAME_ORG_OTHERSBR',
                                'FIRST_NAME_OTHERSBR',
                                'MIDDLE_NAME_OTHERSBR',
                                'NAME_PREFIX_OTHERSBR',
                                'NAME_SUFFIX_OTHERSBR',
                                'ID_CODE_QUALIFIER_OTHERSBR',
                                'ID_CODE_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            NAME_CODE_OTHERSBR,
                            ENTITY_IDENTIFIER_CODE_OTHERSBR,
                            ENTITY_TYPE_QUALIFIER_OTHERSBR,
                            LAST_NAME_ORG_OTHERSBR,
                            FIRST_NAME_OTHERSBR,
                            MIDDLE_NAME_OTHERSBR,
                            NAME_PREFIX_OTHERSBR,
                            NAME_SUFFIX_OTHERSBR,
                            ID_CODE_QUALIFIER_OTHERSBR,
                            ID_CODE_OTHERSBR
                        )
        )
        , claim_sbr_nmIL_n3 as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERSBR'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_OTHERSBR',
                                'ADDRESS_LINE_1_OTHERSBR',
                                'ADDRESS_LINE_2_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            ADDRESS_CODE_OTHERSBR,
                            ADDRESS_LINE_1_OTHERSBR,
                            ADDRESS_LINE_2_OTHERSBR
                        )
        )
        , claim_sbr_nmIL_n4 as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERSBR'
                                    when    flattened.index = 2   then      'CITY_OTHERSBR'
                                    when    flattened.index = 3   then      'ST_OTHERSBR'
                                    when    flattened.index = 4   then      'ZIP_OTHERSBR'
                                    when    flattened.index = 5   then      'COUNTRY_OTHERSBR'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERSBR'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_clm_sbr.lag_name_indicator = 'NM1*IL'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_OTHERSBR',
                                'CITY_OTHERSBR',
                                'ST_OTHERSBR',
                                'ZIP_OTHERSBR',
                                'COUNTRY_OTHERSBR',
                                'LOCATION_QUALIFIER_OTHERSBR',
                                'LOCATION_IDENTIFIER_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            ADDRESS_CODE_OTHERSBR,
                            CITY_OTHERSBR,
                            ST_OTHERSBR,
                            ZIP_OTHERSBR,
                            COUNTRY_OTHERSBR,
                            LOCATION_QUALIFIER_OTHERSBR,
                            LOCATION_IDENTIFIER_OTHERSBR
                        )
        )
        , claim_sbr_nmPR as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_OTHERPYR'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OTHERPYR'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OTHERPYR'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_OTHERPYR'
                                    when    flattened.index = 5   then      'FIRST_NAME_OTHERPYR'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_OTHERPYR'
                                    when    flattened.index = 7   then      'NAME_PREFIX_OTHERPYR'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_OTHERPYR'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OTHERPYR'
                                    when    flattened.index = 10  then      'ID_CODE_OTHERSBR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^NM1\\*PR.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_OTHERPYR',
                                'ENTITY_IDENTIFIER_CODE_OTHERPYR',
                                'ENTITY_TYPE_QUALIFIER_OTHERPYR',
                                'LAST_NAME_ORG_OTHERPYR',
                                'FIRST_NAME_OTHERPYR',
                                'MIDDLE_NAME_OTHERPYR',
                                'NAME_PREFIX_OTHERPYR',
                                'NAME_SUFFIX_OTHERPYR',
                                'ID_CODE_QUALIFIER_OTHERPYR',
                                'ID_CODE_OTHERSBR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            NAME_CODE_OTHERPYR,
                            ENTITY_IDENTIFIER_CODE_OTHERPYR,
                            ENTITY_TYPE_QUALIFIER_OTHERPYR,
                            LAST_NAME_ORG_OTHERPYR,
                            FIRST_NAME_OTHERPYR,
                            MIDDLE_NAME_OTHERPYR,
                            NAME_PREFIX_OTHERPYR,
                            NAME_SUFFIX_OTHERPYR,
                            ID_CODE_QUALIFIER_OTHERPYR,
                            ID_CODE_OTHERSBR
                        )
        )
        , claim_sbr_nmPR_n3 as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_OTHERPYR'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_OTHERPYR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_OTHERPYR',
                                'ADDRESS_LINE_1_OTHERPYR',
                                'ADDRESS_LINE_2_OTHERPYR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            ADDRESS_CODE_OTHERPYR,
                            ADDRESS_LINE_1_OTHERPYR,
                            ADDRESS_LINE_2_OTHERPYR
                        )
        )
        , claim_sbr_nmPR_n4 as
        (
            with long as
            (
                select      filtered_clm_sbr.id_837,
                            filtered_clm_sbr.nth_transaction_set,
                            filtered_clm_sbr.index,
                            filtered_clm_sbr.hl_index_current,
                            filtered_clm_sbr.hl_index_billing_20,
                            filtered_clm_sbr.hl_index_subscriber_22,
                            filtered_clm_sbr.hl_index_patient_23,
                            filtered_clm_sbr.claim_index,
                            filtered_clm_sbr.other_sbr_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_OTHERPYR'
                                    when    flattened.index = 2   then      'CITY_OTHERPYR'
                                    when    flattened.index = 3   then      'ST_OTHERPYR'
                                    when    flattened.index = 4   then      'ZIP_OTHERPYR'
                                    when    flattened.index = 5   then      'COUNTRY_OTHERPYR'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_OTHERPYR'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_OTHERPYR'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm_sbr,
                            lateral split_to_table(filtered_clm_sbr.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm_sbr.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_clm_sbr.lag_name_indicator = 'NM1*PR'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_OTHERPYR',
                                'CITY_OTHERPYR',
                                'ST_OTHERPYR',
                                'ZIP_OTHERPYR',
                                'COUNTRY_OTHERPYR',
                                'LOCATION_QUALIFIER_OTHERPYR',
                                'LOCATION_IDENTIFIER_OTHERPYR'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            CLAIM_INDEX,
                            OTHER_SBR_INDEX,
                            ADDRESS_CODE_OTHERPYR,
                            CITY_OTHERPYR,
                            ST_OTHERPYR,
                            ZIP_OTHERPYR,
                            COUNTRY_OTHERPYR,
                            LOCATION_QUALIFIER_OTHERPYR,
                            LOCATION_IDENTIFIER_OTHERPYR
                        )
        )
        select      header.id_837,
                    header.nth_transaction_set,
                    header.index,
                    header.hl_index_current,
                    header.hl_index_billing_20,
                    header.hl_index_subscriber_22,
                    header.hl_index_patient_23,
                    header.claim_index,
                    header.other_sbr_index,
                    header.sbr_prefix_othersbr,
                    header.payor_responsibility_sequence_othersbr,
                    header.individual_relationship_code_othersbr,
                    header.group_number_othersbr,
                    header.group_name_othersbr,
                    header.insurance_type_code_othersbr,
                    header.coordination_of_benefits_code_othersbr,
                    header.employment_code_othersbr,
                    header.claim_filing_indicator_code_othersbr,
                    header.patient_signature_source_code_othersbr,
                    oi.prefix_othersbr,
                    oi.empty1_othersbr,
                    oi.empty2_othersbr,
                    oi.benefits_assignment_othersbr,
                    oi.patient_signature_source_othersbr,
                    oi.empty5_othersbr,
                    oi.release_of_info_othersbr,
                    nmIL.name_code_othersbr,
                    nmIL.entity_identifier_code_othersbr,
                    nmIL.entity_type_qualifier_othersbr,
                    nmIL.last_name_org_othersbr,
                    nmIL.first_name_othersbr,
                    nmIL.middle_name_othersbr,
                    nmIL.name_prefix_othersbr,
                    nmIL.name_suffix_othersbr,
                    nmIL.id_code_qualifier_othersbr,
                    nmIL.id_code_othersbr,
                    nmIL_n3.address_code_othersbr,
                    nmIL_n3.address_line_1_othersbr,
                    nmIL_n3.address_line_2_othersbr,
                    nmIL_n4.address_code_othersbr,
                    nmIL_n4.city_othersbr,
                    nmIL_n4.st_othersbr,
                    nmIL_n4.zip_othersbr,
                    nmIL_n4.country_othersbr,
                    nmIL_n4.location_qualifier_othersbr,
                    nmIL_n4.location_identifier_othersbr,
                    nmPR.name_code_otherpyr,
                    nmPR.entity_identifier_code_otherpyr,
                    nmPR.entity_type_qualifier_otherpyr,
                    nmPR.last_name_org_otherpyr,
                    nmPR.first_name_otherpyr,
                    nmPR.middle_name_otherpyr,
                    nmPR.name_prefix_otherpyr,
                    nmPR.name_suffix_otherpyr,
                    nmPR.id_code_qualifier_otherpyr,
                    nmPR.id_code_othersbr,
                    nmPR_n3.address_code_otherpyr,
                    nmPR_n3.address_line_1_otherpyr,
                    nmPR_n3.address_line_2_otherpyr,
                    nmPR_n3.address_code_otherpyr,
                    nmPR_n4.city_otherpyr,
                    nmPR_n4.st_otherpyr,
                    nmPR_n4.zip_otherpyr,
                    nmPR_n4.country_otherpyr,
                    nmPR_n4.location_qualifier_otherpyr,
                    nmPR_n4.location_identifier_otherpyr,

                    cas.cas_adj_array,
                    amt.amt_adj_array

        from        claim_sbr_header    as header
                    left join
                        claim_sbr_oi        as oi
                        on  header.id_837               = oi.id_837
                        and header.nth_transaction_set  = oi.nth_transaction_set
                        and header.claim_index          = oi.claim_index
                        and header.other_sbr_index      = oi.other_sbr_index
                    left join
                        claim_sbr_nmIL      as nmIL
                        on  header.id_837               = nmIL.id_837
                        and header.nth_transaction_set  = nmIL.nth_transaction_set
                        and header.claim_index          = nmIL.claim_index
                        and header.other_sbr_index      = nmIL.other_sbr_index
                    left join
                        claim_sbr_nmIL_n3   as nmIL_n3
                        on  header.id_837               = nmIL_n3.id_837
                        and header.nth_transaction_set  = nmIL_n3.nth_transaction_set
                        and header.claim_index          = nmIL_n3.claim_index
                        and header.other_sbr_index      = nmIL_n3.other_sbr_index
                    left join
                        claim_sbr_nmIL_n4   as nmIL_n4
                        on  header.id_837               = nmIL_n4.id_837
                        and header.nth_transaction_set  = nmIL_n4.nth_transaction_set
                        and header.claim_index          = nmIL_n4.claim_index
                        and header.other_sbr_index      = nmIL_n4.other_sbr_index
                    left join
                        claim_sbr_nmPR      as nmPR
                        on  header.id_837               = nmPR.id_837
                        and header.nth_transaction_set  = nmPR.nth_transaction_set
                        and header.claim_index          = nmPR.claim_index
                        and header.other_sbr_index      = nmPR.other_sbr_index
                    left join
                        claim_sbr_nmPR_n3   as nmPR_n3
                        on  header.id_837               = nmPR_n3.id_837
                        and header.nth_transaction_set  = nmPR_n3.nth_transaction_set
                        and header.claim_index          = nmPR_n3.claim_index
                        and header.other_sbr_index      = nmPR_n3.other_sbr_index
                    left join
                        claim_sbr_nmPR_n4   as nmPR_n4
                        on  header.id_837               = nmPR_n4.id_837
                        and header.nth_transaction_set  = nmPR_n4.nth_transaction_set
                        and header.claim_index          = nmPR_n4.claim_index
                        and header.other_sbr_index      = nmPR_n4.other_sbr_index
                        
                    left join
                        claim_sbr_cas       as cas
                        on  header.id_837               = cas.id_837
                        and header.nth_transaction_set  = cas.nth_transaction_set
                        and header.claim_index          = cas.claim_index
                        and header.other_sbr_index      = cas.other_sbr_index
                    left join
                        claim_sbr_amt       as amt
                        on  header.id_837               = amt.id_837
                        and header.nth_transaction_set  = amt.nth_transaction_set
                        and header.claim_index          = amt.claim_index
                        and header.other_sbr_index      = amt.other_sbr_index
    )
    select      *
    from        clm_othersbr
)
select      *
from        parse_transaction_sets
;