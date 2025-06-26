create or replace table
    edwprodhh.edi_837_parser.header_functional_group
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
, parse_functional_group_header as
(
    with labeled as
    (
        select      flatten_837.id_837,
        
                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1     then    'FUNCTIONAL_GROUP_HEADER'
                            when    flattened.index = 2     then    'FUNCTIONAL_IDENTIFIER_CODE'
                            when    flattened.index = 3     then    'APPLICATION_SENDER_CODE'
                            when    flattened.index = 4     then    'APPLICATION_RECEIVER_CODE'
                            when    flattened.index = 5     then    'FUNCTIONAL_GROUP_CREATED_DATE'
                            when    flattened.index = 6     then    'FUNCTIONAL_GROUP_CREATED_TIME'
                            when    flattened.index = 7     then    'CONTROL_GROUP_NUMBER'
                            when    flattened.index = 8     then    'RESPONSIBLE_AGENCY_CODE'
                            when    flattened.index = 9     then    'VERSION_IDENTIFIER_CODE'
                            end     as value_header,

                    case    when    value_header = 'FUNCTIONAL_GROUP_CREATED_DATE'  then    to_date(nullif(trim(flattened.value), ''), 'YYYYMMDD')::text
                            when    value_header = 'FUNCTIONAL_GROUP_CREATED_TIME'  then    to_time(nullif(trim(flattened.value), ''), 'HH24MISS')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format


        from        flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^GS.*')                          --1 Filter
    )
    select      *,
                (functional_group_created_date || ' ' || functional_group_created_time)::timestamp as functional_group_created_timestamp
    from        labeled
                pivot(
                    max(value_format) for value_header in (
                        'FUNCTIONAL_GROUP_HEADER',
                        'FUNCTIONAL_IDENTIFIER_CODE',
                        'APPLICATION_SENDER_CODE',
                        'APPLICATION_RECEIVER_CODE',
                        'FUNCTIONAL_GROUP_CREATED_DATE',
                        'FUNCTIONAL_GROUP_CREATED_TIME',
                        'CONTROL_GROUP_NUMBER',
                        'RESPONSIBLE_AGENCY_CODE',
                        'VERSION_IDENTIFIER_CODE'
                    )
                )   as pvt (
                    ID_837,
                    FUNCTIONAL_GROUP_HEADER,
                    FUNCTIONAL_IDENTIFIER_CODE,
                    APPLICATION_SENDER_CODE,
                    APPLICATION_RECEIVER_CODE,
                    FUNCTIONAL_GROUP_CREATED_DATE,
                    FUNCTIONAL_GROUP_CREATED_TIME,
                    CONTROL_GROUP_NUMBER,
                    RESPONSIBLE_AGENCY_CODE,
                    VERSION_IDENTIFIER_CODE
                )
)
select      *
from        parse_functional_group_header
;