create or replace table
    edwprodhh.edi_837_parser.claim_service_lines
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
    , clm_service_line_lx as
    (
        with filtered_lx as
        (
            select      *
            from        filtered
            where       claim_index is not null --0 Pre-Filter
                        and lx_index is not null
        )
        , servline_lx_header as
        (
            with long as
            (
                select      filtered_lx.id_837,
                            filtered_lx.nth_transaction_set,
                            filtered_lx.index,
                            filtered_lx.hl_index_current,
                            filtered_lx.hl_index_billing_20,
                            filtered_lx.hl_index_subscriber_22,
                            filtered_lx.hl_index_patient_23,
                            filtered_lx.claim_index,
                            filtered_lx.lx_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'LX_PREFIX'
                                    when    flattened.index = 2   then      'LX_ASSIGNED_LINE_NUMBER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_lx,
                            lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_lx.line_element_837, '^LX.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'LX_PREFIX',
                                'LX_ASSIGNED_LINE_NUMBER'
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
                            LX_INDEX,
                            LX_PREFIX,
                            LX_ASSIGNED_LINE_NUMBER
                        )
        )
        , servline_lx_sv2 as
        (
            with long as
            (
                select      filtered_lx.id_837,
                            filtered_lx.nth_transaction_set,
                            filtered_lx.index,
                            filtered_lx.hl_index_current,
                            filtered_lx.hl_index_billing_20,
                            filtered_lx.hl_index_subscriber_22,
                            filtered_lx.hl_index_patient_23,
                            filtered_lx.claim_index,
                            filtered_lx.lx_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'SV2_PREFIX'
                                    when    flattened.index = 2   then      'REVENUE_CODE'
                                    when    flattened.index = 3   then      'PROCEDURE_CODE'
                                    when    flattened.index = 4   then      'CHARGE_AMOUNT'
                                    when    flattened.index = 5   then      'MEASUREMENT_CODE'
                                    when    flattened.index = 6   then      'SERVICE_UNITS'
                                    when    flattened.index = 7   then      'SV2_MOD_1'
                                    when    flattened.index = 8   then      'SV2_MOD_2'
                                    when    flattened.index = 9   then      'SV2_MOD_3'
                                    when    flattened.index = 10  then      'SV2_MOD_4'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_lx,
                            lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_lx.line_element_837, '^SV2.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'SV2_PREFIX',
                                'REVENUE_CODE',
                                'PROCEDURE_CODE',
                                'CHARGE_AMOUNT',
                                'MEASUREMENT_CODE',
                                'SERVICE_UNITS',
                                'SV2_MOD_1',
                                'SV2_MOD_2',
                                'SV2_MOD_3',
                                'SV2_MOD_4'
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
                            LX_INDEX,
                            SV2_PREFIX,
                            REVENUE_CODE,
                            PROCEDURE_CODE,
                            CHARGE_AMOUNT,
                            MEASUREMENT_CODE,
                            SERVICE_UNITS,
                            SV2_MOD_1,
                            SV2_MOD_2,
                            SV2_MOD_3,
                            SV2_MOD_4
                        )
        )
        , servline_lx_dtp as
        (
            with long as
            (
                select      filtered_lx.id_837,
                            filtered_lx.nth_transaction_set,
                            filtered_lx.index,
                            filtered_lx.hl_index_current,
                            filtered_lx.hl_index_billing_20,
                            filtered_lx.hl_index_subscriber_22,
                            filtered_lx.hl_index_patient_23,
                            filtered_lx.claim_index,
                            filtered_lx.lx_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'DTP_PREFIX_LX'
                                    when    flattened.index = 2   then      'DATE_QUALIFIER_LX'
                                    when    flattened.index = 3   then      'DATE_FORMAT_LX'
                                    when    flattened.index = 4   then      'DATE_LX'
                                    end     as value_header,

                            case    when    value_header = 'DATE_LX'
                                    then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::varchar
                                    else    nullif(trim(flattened.value), '')
                                    end     as value_format

                from        filtered_lx,
                            lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_lx.line_element_837, '^DTP.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'DTP_PREFIX_LX',
                                'DATE_QUALIFIER_LX',
                                'DATE_FORMAT_LX',
                                'DATE_LX'
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
                            LX_INDEX,
                            DTP_PREFIX_LX,
                            DATE_QUALIFIER_LX,
                            DATE_FORMAT_LX,
                            DATE_LX
                        )
        )
        , servline_lx_ref as
        (
            with long as
            (
                select      filtered_lx.id_837,
                            filtered_lx.nth_transaction_set,
                            filtered_lx.index,
                            filtered_lx.hl_index_current,
                            filtered_lx.hl_index_billing_20,
                            filtered_lx.hl_index_subscriber_22,
                            filtered_lx.hl_index_patient_23,
                            filtered_lx.claim_index,
                            filtered_lx.lx_index,

                            -- flattened.index,
                            -- nullif(trim(flattened.value), '') as value_raw,

                            case    when    flattened.index = 1   then      'REF_PREFIX_LX'
                                    when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_LX'
                                    when    flattened.index = 3   then      'REFERENCE_ID_LX'
                                    when    flattened.index = 4   then      'DESCRIPTION_LX'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_lx,
                            lateral split_to_table(filtered_lx.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_lx.line_element_837, '^REF.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'REF_PREFIX_LX',
                                'REFERENCE_ID_QUALIFIER_LX',
                                'REFERENCE_ID_LX',
                                'DESCRIPTION_LX'
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
                            LX_INDEX,
                            REF_PREFIX_LX,
                            REFERENCE_ID_QUALIFIER_LX,
                            REFERENCE_ID_LX,
                            DESCRIPTION_LX
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
                    header.lx_index,
                    header.lx_prefix,
                    header.lx_assigned_line_number,
                    sv2.sv2_prefix,
                    sv2.revenue_code,
                    sv2.procedure_code,
                    sv2.charge_amount,
                    sv2.measurement_code,
                    sv2.service_units,
                    sv2.sv2_mod_1,
                    sv2.sv2_mod_2,
                    sv2.sv2_mod_3,
                    sv2.sv2_mod_4,
                    dtp.dtp_prefix_lx,
                    dtp.date_qualifier_lx,
                    dtp.date_format_lx,
                    dtp.date_lx,
                    ref.ref_prefix_lx,
                    ref.reference_id_qualifier_lx,
                    ref.reference_id_lx,
                    ref.description_lx

        from        servline_lx_header as header
                    left join
                        servline_lx_sv2 as sv2
                        on  header.id_837               = sv2.id_837
                        and header.nth_transaction_set  = sv2.nth_transaction_set
                        and header.claim_index          = sv2.claim_index
                        and header.lx_index             = sv2.lx_index
                    left join
                        servline_lx_dtp as dtp
                        on  header.id_837               = dtp.id_837
                        and header.nth_transaction_set  = dtp.nth_transaction_set
                        and header.claim_index          = dtp.claim_index
                        and header.lx_index             = dtp.lx_index
                    left join
                        servline_lx_ref as ref
                        on  header.id_837               = ref.id_837
                        and header.nth_transaction_set  = ref.nth_transaction_set
                        and header.claim_index          = ref.claim_index
                        and header.lx_index             = ref.lx_index
    )
    select      *
    from        clm_service_line_lx
)
select      *
from        parse_transaction_sets
;