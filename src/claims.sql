create or replace table
    edwprodhh.edi_837_parser.claims
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
    , claim_clm as
    (
        with filtered_clm as
        (
            select      *
            from        filtered
            where       claim_index is not null --0 Pre-Filter
        )
        , header_clm as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'CLM_PREFIX'
                                    when    flattened.index = 2   then      'CLAIM_ID'
                                    when    flattened.index = 3   then      'TOTAL_CLAIM_CHARGE'
                                    when    flattened.index = 4   then      'CLAIM_PAYMENT_AMOUNT'
                                    when    flattened.index = 5   then      'PATIENT_RESPONSIBILITY_AMOUNT'
                                    when    flattened.index = 6   then      'COMPOSITE_FACILITY_CODE'           --11/13/21 Office/Hospital/Inpatient : ...
                                    when    flattened.index = 7   then      'PROVIDER_SIGNATURE'
                                    when    flattened.index = 8   then      'PARTICIPATION_CODE'
                                    when    flattened.index = 9   then      'BENEFITS_ASSIGNMENT_INDICATOR'
                                    when    flattened.index = 10  then      'RELEASE_OF_INFO_CODE'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^CLM.*')                          --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'CLM_PREFIX',
                                'CLAIM_ID',
                                'TOTAL_CLAIM_CHARGE',
                                'CLAIM_PAYMENT_AMOUNT',
                                'PATIENT_RESPONSIBILITY_AMOUNT',
                                'COMPOSITE_FACILITY_CODE',
                                'PROVIDER_SIGNATURE',
                                'PARTICIPATION_CODE',
                                'BENEFITS_ASSIGNMENT_INDICATOR',
                                'RELEASE_OF_INFO_CODE'
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
                            CLM_PREFIX,
                            CLAIM_ID,
                            TOTAL_CLAIM_CHARGE,
                            CLAIM_PAYMENT_AMOUNT,
                            PATIENT_RESPONSIBILITY_AMOUNT,
                            COMPOSITE_FACILITY_CODE,
                            PROVIDER_SIGNATURE,
                            PARTICIPATION_CODE,
                            BENEFITS_ASSIGNMENT_INDICATOR,
                            RELEASE_OF_INFO_CODE
                        )
        )
        , claim_dtp as
        (
            with qualified as
            (
                select      *,
                            lag(line_element_837, 1) over (partition by id_837, nth_transaction_set, claim_index order by index asc) as previous_line_element
                from        filtered_clm
                qualify     left(previous_line_element, 3) = 'CLM'
            )
            , long as
            (
                select      qualified.id_837,
                            qualified.nth_transaction_set,
                            qualified.index,
                            qualified.hl_index_current,
                            qualified.hl_index_billing_20,
                            qualified.hl_index_subscriber_22,
                            qualified.hl_index_patient_23,
                            qualified.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'DTP_PREFIX_CLAIM'
                                    when    flattened.index = 2   then      'DATE_QUALIFIER_CLAIM'
                                    when    flattened.index = 3   then      'DATE_FORMAT_CLAIM'
                                    when    flattened.index = 4   then      'DATE_RANGE_CLAIM'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        qualified,
                            lateral split_to_table(qualified.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(qualified.line_element_837, '^DTP.*')                               --1 Filter
            )
            select      *,
                        to_date(left(date_range_claim,  8), 'YYYYMMDD') as start_date_claim,
                        to_date(right(date_range_claim, 8), 'YYYYMMDD') as end_date_claim
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'DTP_PREFIX_CLAIM',
                                'DATE_QUALIFIER_CLAIM',
                                'DATE_FORMAT_CLAIM',
                                'DATE_RANGE_CLAIM'
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
                            DTP_PREFIX_CLAIM,
                            DATE_QUALIFIER_CLAIM,
                            DATE_FORMAT_CLAIM,
                            DATE_RANGE_CLAIM
                        )
        )
        , claim_cl1 as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'CL1_PREFIX'                
                                    when    flattened.index = 2   then      'ADMISSION_TYPE_CODE'       --1/2/3/4/5 Emergency/Urgent/Elective/Newbord/Trauma
                                    when    flattened.index = 3   then      'ADMISSION_SOURCE_CODE'     --1/2/3/7/9 Phys Referral/Clinic Referral/HMO Referral/ER/Unavailable
                                    when    flattened.index = 4   then      'PATIENT_STATUS_CODE'       --1/2/7/20/30 Discharge home/Discharge short term hospital/Left/Expired/Still a patient
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^CL1.*')                          --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'CL1_PREFIX',
                                'ADMISSION_TYPE_CODE',
                                'ADMISSION_SOURCE_CODE',
                                'PATIENT_STATUS_CODE'
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
                            CL1_PREFIX,
                            ADMISSION_TYPE_CODE,
                            ADMISSION_SOURCE_CODE,
                            PATIENT_STATUS_CODE
                        )
        )
        , claim_ref as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'REF_PREFIX_CLAIM'
                                    when    flattened.index = 2   then      'REFERENCE_ID_CODE_CLAIM'       --D9/EA CLAIM NUM/MEDICAL RECORD NUM
                                    when    flattened.index = 3   then      'REFERENCE_ID_CLAIM'
                                    when    flattened.index = 4   then      'DESCRIPTION_CLAIM'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^REF.*')                            --1 Filter
                            and filtered_clm.claim_index is not null
                            and filtered_clm.lx_index is null
            )
            , pivoted as
            (
                select      *
                from        long
                            pivot(
                                max(value_format) for value_header in (
                                    'REF_PREFIX_CLAIM',
                                    'REFERENCE_ID_CODE_CLAIM',
                                    'REFERENCE_ID_CLAIM',
                                    'DESCRIPTION_CLAIM'
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
                                REF_PREFIX_CLAIM,
                                REFERENCE_ID_CODE_CLAIM,
                                REFERENCE_ID_CLAIM,
                                DESCRIPTION_CLAIM
                            )
            )
            select      id_837,
                        nth_transaction_set,
                        claim_index,
                        array_agg(
                            object_construct_keep_null(
                                'claim_ref_code',           reference_id_code_claim::varchar,
                                'claim_ref_value',          reference_id_claim::varchar,
                                'claim_ref_description',    description_claim::varchar
                            )
                        )   as clm_ref_array
            from        pivoted
            group by    1,2,3
            order by    1,2,3
        )
        , claim_hi as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'HI_PREFIX'
                                    when    flattened.index = 2   then      'HI_VAL01'
                                    when    flattened.index = 3   then      'HI_VAL02'
                                    when    flattened.index = 4   then      'HI_VAL03'
                                    when    flattened.index = 5   then      'HI_VAL04'
                                    when    flattened.index = 6   then      'HI_VAL05'
                                    when    flattened.index = 7   then      'HI_VAL06'
                                    when    flattened.index = 8   then      'HI_VAL07'
                                    when    flattened.index = 9   then      'HI_VAL08'
                                    when    flattened.index = 10  then      'HI_VAL09'
                                    when    flattened.index = 11  then      'HI_VAL10'
                                    when    flattened.index = 12  then      'HI_VAL11'
                                    when    flattened.index = 13  then      'HI_VAL12'
                                    when    flattened.index = 14  then      'HI_VAL13'
                                    when    flattened.index = 15  then      'HI_VAL14'
                                    when    flattened.index = 16  then      'HI_VAL15'
                                    when    flattened.index = 17  then      'HI_VAL16'
                                    when    flattened.index = 18  then      'HI_VAL17'
                                    when    flattened.index = 19  then      'HI_VAL18'
                                    when    flattened.index = 20 then       'HI_VAL19'
                                    when    flattened.index = 21  then      'HI_VAL20'
                                    when    flattened.index = 22  then      'HI_VAL21'
                                    when    flattened.index = 23  then      'HI_VAL22'
                                    when    flattened.index = 24  then      'HI_VAL23'
                                    when    flattened.index = 25  then      'HI_VAL24'
                                    when    flattened.index = 26  then      'HI_VAL25'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^HI.*')                            --1 Filter
            )
            , pivoted as
            (
                select      *
                from        long
                            pivot(
                                max(value_format) for value_header in (
                                    'HI_PREFIX',
                                    'HI_VAL01',
                                    'HI_VAL02',
                                    'HI_VAL03',
                                    'HI_VAL04',
                                    'HI_VAL05',
                                    'HI_VAL06',
                                    'HI_VAL07',
                                    'HI_VAL08',
                                    'HI_VAL09',
                                    'HI_VAL10',
                                    'HI_VAL11',
                                    'HI_VAL12',
                                    'HI_VAL13',
                                    'HI_VAL14',
                                    'HI_VAL15',
                                    'HI_VAL16',
                                    'HI_VAL17',
                                    'HI_VAL18',
                                    'HI_VAL19',
                                    'HI_VAL20',
                                    'HI_VAL21',
                                    'HI_VAL22',
                                    'HI_VAL23',
                                    'HI_VAL24',
                                    'HI_VAL25'
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
                                HI_PREFIX,
                                HI_VAL01,
                                HI_VAL02,
                                HI_VAL03,
                                HI_VAL04,
                                HI_VAL05,
                                HI_VAL06,
                                HI_VAL07,
                                HI_VAL08,
                                HI_VAL09,
                                HI_VAL10,
                                HI_VAL11,
                                HI_VAL12,
                                HI_VAL13,
                                HI_VAL14,
                                HI_VAL15,
                                HI_VAL16,
                                HI_VAL17,
                                HI_VAL18,
                                HI_VAL19,
                                HI_VAL20,
                                HI_VAL21,
                                HI_VAL22,
                                HI_VAL23,
                                HI_VAL24,
                                HI_VAL25
                            )
            )
            select      id_837,
                        nth_transaction_set,
                        claim_index,
                        array_agg(unpvt.metric_value) as clm_hi_array
            from        pivoted
                        unpivot (
                            metric_value for metric_name in (
                                HI_VAL01,
                                HI_VAL02,
                                HI_VAL03,
                                HI_VAL04,
                                HI_VAL05,
                                HI_VAL06,
                                HI_VAL07,
                                HI_VAL08,
                                HI_VAL09,
                                HI_VAL10,
                                HI_VAL11,
                                HI_VAL12,
                                HI_VAL13,
                                HI_VAL14,
                                HI_VAL15,
                                HI_VAL16,
                                HI_VAL17,
                                HI_VAL18,
                                HI_VAL19,
                                HI_VAL20,
                                HI_VAL21,
                                HI_VAL22,
                                HI_VAL23,
                                HI_VAL24,
                                HI_VAL25
                            )
                        )   as unpvt
            group by    1,2,3
            order by    1,2,3
        )
        , claim_nm71 as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_ATTENDING'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_ATTENDING'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_ATTENDING'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_ATTENDING'
                                    when    flattened.index = 5   then      'FIRST_NAME_ATTENDING'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_ATTENDING'
                                    when    flattened.index = 7   then      'NAME_PREFIX_ATTENDING'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_ATTENDING'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_ATTENDING'
                                    when    flattened.index = 10  then      'ID_CODE_ATTENDING'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^NM1\\*71.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_ATTENDING',
                                'ENTITY_IDENTIFIER_CODE_ATTENDING',
                                'ENTITY_TYPE_QUALIFIER_ATTENDING',
                                'LAST_NAME_ORG_ATTENDING',
                                'FIRST_NAME_ATTENDING',
                                'MIDDLE_NAME_ATTENDING',
                                'NAME_PREFIX_ATTENDING',
                                'NAME_SUFFIX_ATTENDING',
                                'ID_CODE_QUALIFIER_ATTENDING',
                                'ID_CODE_ATTENDING'
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
                            NAME_CODE_ATTENDING,
                            ENTITY_IDENTIFIER_CODE_ATTENDING,
                            ENTITY_TYPE_QUALIFIER_ATTENDING,
                            LAST_NAME_ORG_ATTENDING,
                            FIRST_NAME_ATTENDING,
                            MIDDLE_NAME_ATTENDING,
                            NAME_PREFIX_ATTENDING,
                            NAME_SUFFIX_ATTENDING,
                            ID_CODE_QUALIFIER_ATTENDING,
                            ID_CODE_ATTENDING
                        )
        )
        , claim_nm71_prv as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'PRV_PREFIX_ATTENDING'
                                    when    flattened.index = 2   then      'PROVIDER_CODE_ATTENDING'
                                    when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_ATTENDING'
                                    when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_ATTENDING'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                            and filtered_clm.lag_name_indicator = 'NM1*71'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PRV_PREFIX_ATTENDING',
                                'PROVIDER_CODE_ATTENDING',
                                'REFERENCE_ID_QUALIFIER_ATTENDING',
                                'PROVIDER_TAXONOMY_CODE_ATTENDING'
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
                            PRV_PREFIX_ATTENDING,
                            PROVIDER_CODE_ATTENDING,
                            REFERENCE_ID_QUALIFIER_ATTENDING,
                            PROVIDER_TAXONOMY_CODE_ATTENDING
                        )
        )
        , claim_nm72 as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'NAME_CODE_OPERATING'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_OPERATING'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_OPERATING'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_OPERATING'
                                    when    flattened.index = 5   then      'FIRST_NAME_OPERATING'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_OPERATING'
                                    when    flattened.index = 7   then      'NAME_PREFIX_OPERATING'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_OPERATING'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_OPERATING'
                                    when    flattened.index = 10  then      'ID_CODE_OPERATING'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened         --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^NM1\\*72.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_OPERATING',
                                'ENTITY_IDENTIFIER_CODE_OPERATING',
                                'ENTITY_TYPE_QUALIFIER_OPERATING',
                                'LAST_NAME_ORG_OPERATING',
                                'FIRST_NAME_OPERATING',
                                'MIDDLE_NAME_OPERATING',
                                'NAME_PREFIX_OPERATING',
                                'NAME_SUFFIX_OPERATING',
                                'ID_CODE_QUALIFIER_OPERATING',
                                'ID_CODE_OPERATING'
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
                            NAME_CODE_OPERATING,
                            ENTITY_IDENTIFIER_CODE_OPERATING,
                            ENTITY_TYPE_QUALIFIER_OPERATING,
                            LAST_NAME_ORG_OPERATING,
                            FIRST_NAME_OPERATING,
                            MIDDLE_NAME_OPERATING,
                            NAME_PREFIX_OPERATING,
                            NAME_SUFFIX_OPERATING,
                            ID_CODE_QUALIFIER_OPERATING,
                            ID_CODE_OPERATING
                        )
        )
        , claim_nm72_prv as
        (
            with long as
            (
                select      filtered_clm.id_837,
                            filtered_clm.nth_transaction_set,
                            filtered_clm.index,
                            filtered_clm.hl_index_current,
                            filtered_clm.hl_index_billing_20,
                            filtered_clm.hl_index_subscriber_22,
                            filtered_clm.hl_index_patient_23,
                            filtered_clm.claim_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'PRV_PREFIX_OPERATING'
                                    when    flattened.index = 2   then      'PROVIDER_CODE_OPERATING'
                                    when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_OPERATING'
                                    when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_OPERATING'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_clm,
                            lateral split_to_table(filtered_clm.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_clm.line_element_837, '^PRV.*')                         --1 Filter
                            and filtered_clm.lag_name_indicator = 'NM1*72'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PRV_PREFIX_OPERATING',
                                'PROVIDER_CODE_OPERATING',
                                'REFERENCE_ID_QUALIFIER_OPERATING',
                                'PROVIDER_TAXONOMY_CODE_OPERATING'
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
                            PRV_PREFIX_OPERATING,
                            PROVIDER_CODE_OPERATING,
                            REFERENCE_ID_QUALIFIER_OPERATING,
                            PROVIDER_TAXONOMY_CODE_OPERATING
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
                    header.clm_prefix,
                    header.claim_id,
                    header.total_claim_charge,
                    header.claim_payment_amount,
                    header.patient_responsibility_amount,
                    header.composite_facility_code,
                    header.provider_signature,
                    header.participation_code,
                    header.benefits_assignment_indicator,
                    header.release_of_info_code,
                    dtp.dtp_prefix_claim,
                    dtp.date_qualifier_claim,
                    dtp.date_format_claim,
                    dtp.date_range_claim,
                    dtp.start_date_claim,
                    dtp.end_date_claim,
                    cl1.cl1_prefix,
                    cl1.admission_type_code,
                    cl1.admission_source_code,
                    cl1.patient_status_code,
                    nm71.name_code_attending,
                    nm71.entity_identifier_code_attending,
                    nm71.entity_type_qualifier_attending,
                    nm71.last_name_org_attending,
                    nm71.first_name_attending,
                    nm71.middle_name_attending,
                    nm71.name_prefix_attending,
                    nm71.name_suffix_attending,
                    nm71.id_code_qualifier_attending,
                    nm71.id_code_attending,
                    nm71_prv.prv_prefix_attending,
                    nm71_prv.provider_code_attending,
                    nm71_prv.reference_id_qualifier_attending,
                    nm71_prv.provider_taxonomy_code_attending,
                    nm72.name_code_operating,
                    nm72.entity_identifier_code_operating,
                    nm72.entity_type_qualifier_operating,
                    nm72.last_name_org_operating,
                    nm72.first_name_operating,
                    nm72.middle_name_operating,
                    nm72.name_prefix_operating,
                    nm72.name_suffix_operating,
                    nm72.id_code_qualifier_operating,
                    nm72.id_code_operating,
                    nm72_prv.prv_prefix_operating,
                    nm72_prv.provider_code_operating,
                    nm72_prv.reference_id_qualifier_operating,
                    nm72_prv.provider_taxonomy_code_operating,

                    ref.clm_ref_array,
                    hi.clm_hi_array

        from        header_clm      as header
                    left join
                        claim_dtp       as dtp
                        on  header.id_837               = dtp.id_837
                        and header.nth_transaction_set  = dtp.nth_transaction_set
                        and header.claim_index          = dtp.claim_index
                    left join
                        claim_cl1       as cl1
                        on  header.id_837               = cl1.id_837
                        and header.nth_transaction_set  = cl1.nth_transaction_set
                        and header.claim_index          = cl1.claim_index
                    left join
                        claim_nm71      as nm71
                        on  header.id_837               = nm71.id_837
                        and header.nth_transaction_set  = nm71.nth_transaction_set
                        and header.claim_index          = nm71.claim_index
                    left join
                        claim_nm71_prv  as nm71_prv
                        on  header.id_837               = nm71_prv.id_837
                        and header.nth_transaction_set  = nm71_prv.nth_transaction_set
                        and header.claim_index          = nm71_prv.claim_index
                    left join
                        claim_nm72      as nm72
                        on  header.id_837               = nm72.id_837
                        and header.nth_transaction_set  = nm72.nth_transaction_set
                        and header.claim_index          = nm72.claim_index
                    left join
                        claim_nm72_prv  as nm72_prv
                        on  header.id_837               = nm72_prv.id_837
                        and header.nth_transaction_set  = nm72_prv.nth_transaction_set
                        and header.claim_index          = nm72_prv.claim_index
                        
                    left join
                        claim_ref       as ref
                        on  header.id_837               = ref.id_837
                        and header.nth_transaction_set  = ref.nth_transaction_set
                        and header.claim_index          = ref.claim_index
                    left join
                        claim_hi        as hi
                        on  header.id_837               = hi.id_837
                        and header.nth_transaction_set  = hi.nth_transaction_set
                        and header.claim_index          = hi.claim_index
    )
    select      *
    from        claim_clm
)
select      *
from        parse_transaction_sets
;