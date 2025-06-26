create or replace table
    edwprodhh.edi_837_parser.hl_patients
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
    , patient_hl23 as
    (
        with filtered_hl as
        (
            select      *
            from        filtered
            where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                        and hl_index_subscriber_22  is not null
                        and hl_index_patient_23     is not null
                        and claim_index             is null
        )
        , patient_hl as
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

                            case    when    flattened.index = 1   then      'HL_PREFIX_PATIENT'
                                    when    flattened.index = 2   then      'HL_ID_PATIENT'
                                    when    flattened.index = 3   then      'HL_PARENT_ID_PATIENT'
                                    when    flattened.index = 4   then      'HL_LEVEL_CODE_PATIENT' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                                    when    flattened.index = 5   then      'HL_CHILD_CODE_PATIENT' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                                'HL_PREFIX_PATIENT',
                                'HL_ID_PATIENT',
                                'HL_PARENT_ID_PATIENT',
                                'HL_LEVEL_CODE_PATIENT',
                                'HL_CHILD_CODE_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            HL_PREFIX_PATIENT,
                            HL_ID_PATIENT,
                            HL_PARENT_ID_PATIENT,
                            HL_LEVEL_CODE_PATIENT,
                            HL_CHILD_CODE_PATIENT
                        )
        )
        , patient_pat as
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

                            case    when    flattened.index = 1   then      'PAT_PREFIX_PATIENT'
                                    when    flattened.index = 2   then      'RELATIONSHIP_CODE_PATIENT'     --18/01/19/20 SELF/SPOUSE/CHILD/EMPLOYEE
                                    when    flattened.index = 3   then      'LOCATION_CODE_PATIENT'
                                    when    flattened.index = 4   then      'EMPLOYMENT_STATUS_PATIENT'
                                    when    flattened.index = 5   then      'STUDENT_STATUS_PATIENT'
                                    when    flattened.index = 6   then      'DATE_OF_DEATH_PATIENT'
                                    when    flattened.index = 7   then      'FORMAT_QUALIFIER_PATIENT'
                                    when    flattened.index = 8   then      'MEASUREMENT_UNIT_CODE_PATIENT'
                                    when    flattened.index = 9   then      'WEIGHT_PATIENT'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^PAT.*')                          --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PAT_PREFIX_PATIENT',
                                'RELATIONSHIP_CODE_PATIENT',
                                'LOCATION_CODE_PATIENT',
                                'EMPLOYMENT_STATUS_PATIENT',
                                'STUDENT_STATUS_PATIENT',
                                'DATE_OF_DEATH_PATIENT',
                                'FORMAT_QUALIFIER_PATIENT',
                                'MEASUREMENT_UNIT_CODE_PATIENT',
                                'WEIGHT_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            PAT_PREFIX_PATIENT,
                            RELATIONSHIP_CODE_PATIENT,
                            LOCATION_CODE_PATIENT,
                            EMPLOYMENT_STATUS_PATIENT,
                            STUDENT_STATUS_PATIENT,
                            DATE_OF_DEATH_PATIENT,
                            FORMAT_QUALIFIER_PATIENT,
                            MEASUREMENT_UNIT_CODE_PATIENT,
                            WEIGHT_PATIENT
                        )
        )
        , patient_nmQC as
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

                            case    when    flattened.index = 1   then      'NAME_CODE_PATIENT'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PATIENT'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PATIENT'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_PATIENT'
                                    when    flattened.index = 5   then      'FIRST_NAME_PATIENT'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_PATIENT'
                                    when    flattened.index = 7   then      'NAME_PREFIX_PATIENT'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_PATIENT'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PATIENT'
                                    when    flattened.index = 10  then      'ID_CODE_PATIENT'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened     --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^NM1\\*QC.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_PATIENT',
                                'ENTITY_IDENTIFIER_CODE_PATIENT',
                                'ENTITY_TYPE_QUALIFIER_PATIENT',
                                'LAST_NAME_ORG_PATIENT',
                                'FIRST_NAME_PATIENT',
                                'MIDDLE_NAME_PATIENT',
                                'NAME_PREFIX_PATIENT',
                                'NAME_SUFFIX_PATIENT',
                                'ID_CODE_QUALIFIER_PATIENT',
                                'ID_CODE_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            NAME_CODE_PATIENT,
                            ENTITY_IDENTIFIER_CODE_PATIENT,
                            ENTITY_TYPE_QUALIFIER_PATIENT,
                            LAST_NAME_ORG_PATIENT,
                            FIRST_NAME_PATIENT,
                            MIDDLE_NAME_PATIENT,
                            NAME_PREFIX_PATIENT,
                            NAME_SUFFIX_PATIENT,
                            ID_CODE_QUALIFIER_PATIENT,
                            ID_CODE_PATIENT
                        )
        )
        , patient_n3 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_PATIENT'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_PATIENT'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*QC'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PATIENT',
                                'ADDRESS_LINE_1_PATIENT',
                                'ADDRESS_LINE_2_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PATIENT,
                            ADDRESS_LINE_1_PATIENT,
                            ADDRESS_LINE_2_PATIENT
                        )
        )
        , patient_n4 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PATIENT'
                                    when    flattened.index = 2   then      'CITY_PATIENT'
                                    when    flattened.index = 3   then      'ST_PATIENT'
                                    when    flattened.index = 4   then      'ZIP_PATIENT'
                                    when    flattened.index = 5   then      'COUNTRY_PATIENT'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_PATIENT'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PATIENT'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*QC'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PATIENT',
                                'CITY_PATIENT',
                                'ST_PATIENT',
                                'ZIP_PATIENT',
                                'COUNTRY_PATIENT',
                                'LOCATION_QUALIFIER_PATIENT',
                                'LOCATION_IDENTIFIER_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PATIENT,
                            CITY_PATIENT,
                            ST_PATIENT,
                            ZIP_PATIENT,
                            COUNTRY_PATIENT,
                            LOCATION_QUALIFIER_PATIENT,
                            LOCATION_IDENTIFIER_PATIENT
                        )
        )
        , patient_dmg as
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

                            case    when    flattened.index = 1   then      'DMG_PREFIX_PATIENT'
                                    when    flattened.index = 2   then      'FORMAT_QUALIFIER_PATIENT'
                                    when    flattened.index = 3   then      'DOB_PATIENT'
                                    when    flattened.index = 4   then      'GENDER_CODE_PATIENT'
                                    when    flattened.index = 5   then      'MARITAL_STATUS_PATIENT'
                                    when    flattened.index = 6   then      'ETHNICITY_CODE_PATIENT'
                                    when    flattened.index = 7   then      'CITIZENSHIP_CODE_PATIENT'
                                    when    flattened.index = 8   then      'COUNTRY_CODE_PATIENT'
                                    when    flattened.index = 9   then      'VERIFICATION_CODE_PATIENT'
                                    when    flattened.index = 10  then      'QUANTITY_PATIENT'
                                    when    flattened.index = 11  then      'LIST_QUALIFIER_CODE_PATIENT'
                                    when    flattened.index = 12  then      'INDUSTRY_CODE_PATIENT'
                                    end     as value_header,

                            case    when    value_header = 'DOB_PATIENT'
                                    then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                                    else    nullif(trim(flattened.value), '')
                                    end     as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^DMG.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*QC'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'DMG_PREFIX_PATIENT',
                                'FORMAT_QUALIFIER_PATIENT',
                                'DOB_PATIENT',
                                'GENDER_CODE_PATIENT',
                                'MARITAL_STATUS_PATIENT',
                                'ETHNICITY_CODE_PATIENT',
                                'CITIZENSHIP_CODE_PATIENT',
                                'COUNTRY_CODE_PATIENT',
                                'VERIFICATION_CODE_PATIENT',
                                'QUANTITY_PATIENT',
                                'LIST_QUALIFIER_CODE_PATIENT',
                                'INDUSTRY_CODE_PATIENT'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            DMG_PREFIX_PATIENT,
                            FORMAT_QUALIFIER_PATIENT,
                            DOB_PATIENT,
                            GENDER_CODE_PATIENT,
                            MARITAL_STATUS_PATIENT,
                            ETHNICITY_CODE_PATIENT,
                            CITIZENSHIP_CODE_PATIENT,
                            COUNTRY_CODE_PATIENT,
                            VERIFICATION_CODE_PATIENT,
                            QUANTITY_PATIENT,
                            LIST_QUALIFIER_CODE_PATIENT,
                            INDUSTRY_CODE_PATIENT
                        )
        )
        select      header.id_837,
                    header.nth_transaction_set,
                    header.index,
                    header.hl_index_current,
                    header.hl_index_billing_20,
                    header.hl_index_subscriber_22,
                    header.hl_index_patient_23,
                    header.hl_prefix_patient,
                    header.hl_id_patient,
                    header.hl_parent_id_patient,
                    header.hl_level_code_patient,
                    header.hl_child_code_patient,
                    pat.pat_prefix_patient,
                    pat.relationship_code_patient,
                    pat.location_code_patient,
                    pat.employment_status_patient,
                    pat.student_status_patient,
                    pat.date_of_death_patient,
                    pat.format_qualifier_patient,
                    pat.measurement_unit_code_patient,
                    pat.weight_patient,
                    nmQC.name_code_patient,
                    nmQC.entity_identifier_code_patient,
                    nmQC.entity_type_qualifier_patient,
                    nmQC.last_name_org_patient,
                    nmQC.first_name_patient,
                    nmQC.middle_name_patient,
                    nmQC.name_prefix_patient,
                    nmQC.name_suffix_patient,
                    nmQC.id_code_qualifier_patient,
                    nmQC.id_code_patient,
                    n3.address_code_patient,
                    n3.address_line_1_patient,
                    n3.address_line_2_patient,
                    n4.address_code_patient,
                    n4.city_patient,
                    n4.st_patient,
                    n4.zip_patient,
                    n4.country_patient,
                    n4.location_qualifier_patient,
                    n4.location_identifier_patient,
                    dmg.dmg_prefix_patient,
                    dmg.format_qualifier_patient,
                    dmg.dob_patient,
                    dmg.gender_code_patient,
                    dmg.marital_status_patient,
                    dmg.ethnicity_code_patient,
                    dmg.citizenship_code_patient,
                    dmg.country_code_patient,
                    dmg.verification_code_patient,
                    dmg.quantity_patient,
                    dmg.list_qualifier_code_patient,
                    dmg.industry_code_patient

        from        patient_hl      as header
                    left join
                        patient_pat     as pat
                        on  header.id_837               = pat.id_837
                        and header.nth_transaction_set  = pat.nth_transaction_set
                        and header.hl_index_patient_23  = pat.hl_index_patient_23
                    left join
                        patient_nmQC    as nmQC
                        on  header.id_837               = nmQC.id_837
                        and header.nth_transaction_set  = nmQC.nth_transaction_set
                        and header.hl_index_patient_23  = nmQC.hl_index_patient_23
                    left join
                        patient_n3      as n3
                        on  header.id_837               = n3.id_837
                        and header.nth_transaction_set  = n3.nth_transaction_set
                        and header.hl_index_patient_23  = n3.hl_index_patient_23
                    left join
                        patient_n4      as n4
                        on  header.id_837               = n4.id_837
                        and header.nth_transaction_set  = n4.nth_transaction_set
                        and header.hl_index_patient_23  = n4.hl_index_patient_23
                    left join
                        patient_dmg     as dmg
                        on  header.id_837               = dmg.id_837
                        and header.nth_transaction_set  = dmg.nth_transaction_set
                        and header.hl_index_patient_23  = dmg.hl_index_patient_23
    )
    select      *
    from        patient_hl23
)
select      *
from        parse_transaction_sets
;