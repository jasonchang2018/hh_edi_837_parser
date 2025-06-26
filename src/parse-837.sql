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
, parse_interchange_control_header as
(
    with labeled as
    (
        select      flatten_837.id_837,

                    -- flattened.index,
                    -- nullif(trim(flattened.value), '') as value_raw,

                    case    when    flattened.index = 1   then      'INTERCHANGE_CONTROL_HEADER'
                            when    flattened.index = 2   then      'AUTHORIZATION_INFORMATION_QUALIFIER'
                            when    flattened.index = 3   then      'AUTHORIZATION_INFORMATION'
                            when    flattened.index = 4   then      'SECURITY_INFORMATION_QUALIFIER'
                            when    flattened.index = 5   then      'SECURITY_INFORMATION'
                            when    flattened.index = 6   then      'INTERCHANGE_ID_QUALIFIER_SENDER'
                            when    flattened.index = 7   then      'INTERCHANGE_SENDER_ID'
                            when    flattened.index = 8   then      'INTERCHANGE_ID_QUALIFIER_RECEIVER'
                            when    flattened.index = 9   then      'INTERCHANGE_RECEIVER_ID'
                            when    flattened.index = 10  then      'INTERCHANGE_DATE'
                            when    flattened.index = 11  then      'INTERCHANGE_TIME'
                            when    flattened.index = 12  then      'REPETITION_SEPARATOR'
                            when    flattened.index = 13  then      'INTERCHANGE_CONTROL_VERSION'
                            when    flattened.index = 14  then      'INTERCHANGE_CONTROL_NUMBER'
                            when    flattened.index = 15  then      'ACKNOWLEDGEMENT_REQUESTED'
                            when    flattened.index = 16  then      'USAGE_INDICATOR'
                            when    flattened.index = 17  then      'COMPONENT_SEPARATOR'
                            end     as value_header,

                    case    when    value_header = 'INTERCHANGE_DATE'   then    to_date(nullif(trim(flattened.value), ''), 'YYMMDD')::text
                            when    value_header = 'INTERCHANGE_TIME'   then    to_time(nullif(trim(flattened.value), ''), 'HH24MI')::text
                            else    nullif(trim(flattened.value), '')
                            end     as value_format

        from        flatten_837,
                    lateral split_to_table(flatten_837.line_element_837, '*') as flattened      --2 Flatten

        where       regexp_like(flatten_837.line_element_837, '^ISA.*')                         --1 Filter
    )
    select      *,
                (interchange_date || ' ' || interchange_time)::timestamp as interchange_timestamp
    from        labeled
                pivot(
                    max(value_format) for value_header in (
                        'INTERCHANGE_CONTROL_HEADER',
                        'AUTHORIZATION_INFORMATION_QUALIFIER',
                        'AUTHORIZATION_INFORMATION',
                        'SECURITY_INFORMATION_QUALIFIER',
                        'SECURITY_INFORMATION',
                        'INTERCHANGE_ID_QUALIFIER_SENDER',
                        'INTERCHANGE_SENDER_ID',
                        'INTERCHANGE_ID_QUALIFIER_RECEIVER',
                        'INTERCHANGE_RECEIVER_ID',
                        'INTERCHANGE_DATE',
                        'INTERCHANGE_TIME',
                        'REPETITION_SEPARATOR',
                        'INTERCHANGE_CONTROL_VERSION',
                        'INTERCHANGE_CONTROL_NUMBER',
                        'ACKNOWLEDGEMENT_REQUESTED',
                        'USAGE_INDICATOR',
                        'COMPONENT_SEPARATOR'
                    )
                )   as pvt (
                    ID_837,
                    INTERCHANGE_CONTROL_HEADER,
                    AUTHORIZATION_INFORMATION_QUALIFIER,
                    AUTHORIZATION_INFORMATION,
                    SECURITY_INFORMATION_QUALIFIER,
                    SECURITY_INFORMATION,
                    INTERCHANGE_ID_QUALIFIER_SENDER,
                    INTERCHANGE_SENDER_ID,
                    INTERCHANGE_ID_QUALIFIER_RECEIVER,
                    INTERCHANGE_RECEIVER_ID,
                    INTERCHANGE_DATE,
                    INTERCHANGE_TIME,
                    REPETITION_SEPARATOR,
                    INTERCHANGE_CONTROL_VERSION,
                    INTERCHANGE_CONTROL_NUMBER,
                    ACKNOWLEDGEMENT_REQUESTED,
                    USAGE_INDICATOR,
                    COMPONENT_SEPARATOR
                )
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
    , billing_hl20 as --JOIN COMPLETED
    (
        with filtered_hl as
        (
            select      *
            from        filtered
            where       hl_index_billing_20         is not null                                          --0 Pre-Filter
                        and hl_index_subscriber_22  is null
                        and hl_index_patient_23     is null
                        and claim_index             is null
        )
        , provider_hl as
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

                            case    when    flattened.index = 1   then      'HL_PREFIX_PROVIDER'
                                    when    flattened.index = 2   then      'HL_ID_PROVIDER'
                                    when    flattened.index = 3   then      'HL_PARENT_ID_PROVIDER'
                                    when    flattened.index = 4   then      'HL_LEVEL_CODE_PROVIDER' --20 BILLING, 22 SUBSCRIBER, 23 PATIENT
                                    when    flattened.index = 5   then      'HL_CHILD_CODE_PROVIDER' --1 HAS CHILD NODE, 0 NO CHILD NODE
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
                                'HL_PREFIX_PROVIDER',
                                'HL_ID_PROVIDER',
                                'HL_PARENT_ID_PROVIDER',
                                'HL_LEVEL_CODE_PROVIDER',
                                'HL_CHILD_CODE_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            HL_PREFIX_PROVIDER,
                            HL_ID_PROVIDER,
                            HL_PARENT_ID_PROVIDER,
                            HL_LEVEL_CODE_PROVIDER,
                            HL_CHILD_CODE_PROVIDER
                        )
        )
        , provider_prv as
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

                            case    when    flattened.index = 1   then      'PRV_PREFIX_PROVIDER'
                                    when    flattened.index = 2   then      'PROVIDER_CODE_PROVIDER'
                                    when    flattened.index = 3   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                                    when    flattened.index = 4   then      'PROVIDER_TAXONOMY_CODE_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^PRV.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PRV_PREFIX_PROVIDER',
                                'PROVIDER_CODE_PROVIDER',
                                'REFERENCE_ID_QUALIFIER_PROVIDER',
                                'PROVIDER_TAXONOMY_CODE_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            PRV_PREFIX_PROVIDER,
                            PROVIDER_CODE_PROVIDER,
                            REFERENCE_ID_QUALIFIER_PROVIDER,
                            PROVIDER_TAXONOMY_CODE_PROVIDER
                        )
        )
        , provider_nm85 as
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

                            case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER'
                                    when    flattened.index = 5   then      'FIRST_NAME_PROVIDER'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER'
                                    when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER'
                                    when    flattened.index = 10  then      'ID_CODE_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^NM1\\*85.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_PROVIDER',
                                'ENTITY_IDENTIFIER_CODE_PROVIDER',
                                'ENTITY_TYPE_QUALIFIER_PROVIDER',
                                'LAST_NAME_ORG_PROVIDER',
                                'FIRST_NAME_PROVIDER',
                                'MIDDLE_NAME_PROVIDER',
                                'NAME_PREFIX_PROVIDER',
                                'NAME_SUFFIX_PROVIDER',
                                'ID_CODE_QUALIFIER_PROVIDER',
                                'ID_CODE_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            NAME_CODE_PROVIDER,
                            ENTITY_IDENTIFIER_CODE_PROVIDER,
                            ENTITY_TYPE_QUALIFIER_PROVIDER,
                            LAST_NAME_ORG_PROVIDER,
                            FIRST_NAME_PROVIDER,
                            MIDDLE_NAME_PROVIDER,
                            NAME_PREFIX_PROVIDER,
                            NAME_SUFFIX_PROVIDER,
                            ID_CODE_QUALIFIER_PROVIDER,
                            ID_CODE_PROVIDER
                        )
        )
        , provider_n3 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*85'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PROVIDER',
                                'ADDRESS_LINE_1_PROVIDER',
                                'ADDRESS_LINE_2_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PROVIDER,
                            ADDRESS_LINE_1_PROVIDER,
                            ADDRESS_LINE_2_PROVIDER
                        )
        )
        , provider_n4 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER'
                                    when    flattened.index = 2   then      'CITY_PROVIDER'
                                    when    flattened.index = 3   then      'ST_PROVIDER'
                                    when    flattened.index = 4   then      'ZIP_PROVIDER'
                                    when    flattened.index = 5   then      'COUNTRY_PROVIDER'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*85'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PROVIDER',
                                'CITY_PROVIDER',
                                'ST_PROVIDER',
                                'ZIP_PROVIDER',
                                'COUNTRY_PROVIDER',
                                'LOCATION_QUALIFIER_PROVIDER',
                                'LOCATION_IDENTIFIER_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PROVIDER,
                            CITY_PROVIDER,
                            ST_PROVIDER,
                            ZIP_PROVIDER,
                            COUNTRY_PROVIDER,
                            LOCATION_QUALIFIER_PROVIDER,
                            LOCATION_IDENTIFIER_PROVIDER
                        )
        )
        , provider_ref as
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
                            case    when    flattened.index = 1   then      'REF_CODE_PROVIDER'
                                    when    flattened.index = 2   then      'REFERENCE_ID_QUALIFIER_PROVIDER'
                                    when    flattened.index = 3   then      'REFERENCE_ID_PROVIDER'
                                    when    flattened.index = 4   then      'DESCRIPTION_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^REF.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*85'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'REF_CODE_PROVIDER',
                                'REFERENCE_ID_QUALIFIER_PROVIDER',
                                'REFERENCE_ID_PROVIDER',
                                'DESCRIPTION_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            REF_CODE_PROVIDER,
                            REFERENCE_ID_QUALIFIER_PROVIDER,
                            REFERENCE_ID_PROVIDER,
                            DESCRIPTION_PROVIDER
                        )
        )
        , provider_per as
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
                            case    when    flattened.index = 1   then      'PROVIDER_CONTACT_PREFIX'
                                    when    flattened.index = 2   then      'CONTACT_FUNCTION_CODE_PROVIDER'
                                    when    flattened.index = 3   then      'CONTACT_NAME_PROVIDER'
                                    when    flattened.index = 4   then      'COMMUNICATION_QUALIFIER_1_PROVIDER'
                                    when    flattened.index = 5   then      'COMMUNICATION_NUMBER_1_PROVIDER'
                                    when    flattened.index = 6   then      'COMMUNICATION_QUALIFIER_2_PROVIDER'
                                    when    flattened.index = 7   then      'COMMUNICATION_NUMBER_2_PROVIDER'
                                    when    flattened.index = 8   then      'COMMUNICATION_QUALIFIER_3_PROVIDER'
                                    when    flattened.index = 9   then      'COMMUNICATION_NUMBER_3_PROVIDER'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^PER.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*85'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'PROVIDER_CONTACT_PREFIX',
                                'CONTACT_FUNCTION_CODE_PROVIDER',
                                'CONTACT_NAME_PROVIDER',
                                'COMMUNICATION_QUALIFIER_1_PROVIDER',
                                'COMMUNICATION_NUMBER_1_PROVIDER',
                                'COMMUNICATION_QUALIFIER_2_PROVIDER',
                                'COMMUNICATION_NUMBER_2_PROVIDER',
                                'COMMUNICATION_QUALIFIER_3_PROVIDER',
                                'COMMUNICATION_NUMBER_3_PROVIDER'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            PROVIDER_CONTACT_PREFIX,
                            CONTACT_FUNCTION_CODE_PROVIDER,
                            CONTACT_NAME_PROVIDER,
                            COMMUNICATION_QUALIFIER_1_PROVIDER,
                            COMMUNICATION_NUMBER_1_PROVIDER,
                            COMMUNICATION_QUALIFIER_2_PROVIDER,
                            COMMUNICATION_NUMBER_2_PROVIDER,
                            COMMUNICATION_QUALIFIER_3_PROVIDER,
                            COMMUNICATION_NUMBER_3_PROVIDER
                        )
        )
        , provider_payto_nm87 as
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

                            case    when    flattened.index = 1   then      'NAME_CODE_PROVIDER_PAYTO'
                                    when    flattened.index = 2   then      'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO'
                                    when    flattened.index = 3   then      'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO'
                                    when    flattened.index = 4   then      'LAST_NAME_ORG_PROVIDER_PAYTO'
                                    when    flattened.index = 5   then      'FIRST_NAME_PROVIDER_PAYTO'
                                    when    flattened.index = 6   then      'MIDDLE_NAME_PROVIDER_PAYTO'
                                    when    flattened.index = 7   then      'NAME_PREFIX_PROVIDER_PAYTO'
                                    when    flattened.index = 8   then      'NAME_SUFFIX_PROVIDER_PAYTO'
                                    when    flattened.index = 9   then      'ID_CODE_QUALIFIER_PROVIDER_PAYTO'
                                    when    flattened.index = 10  then      'ID_CODE_PROVIDER_PAYTO'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened      --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^NM1\\*87.*')                         --1 Filter
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'NAME_CODE_PROVIDER_PAYTO',
                                'ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO',
                                'ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO',
                                'LAST_NAME_ORG_PROVIDER_PAYTO',
                                'FIRST_NAME_PROVIDER_PAYTO',
                                'MIDDLE_NAME_PROVIDER_PAYTO',
                                'NAME_PREFIX_PROVIDER_PAYTO',
                                'NAME_SUFFIX_PROVIDER_PAYTO',
                                'ID_CODE_QUALIFIER_PROVIDER_PAYTO',
                                'ID_CODE_PROVIDER_PAYTO'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            NAME_CODE_PROVIDER_PAYTO,
                            ENTITY_IDENTIFIER_CODE_PROVIDER_PAYTO,
                            ENTITY_TYPE_QUALIFIER_PROVIDER_PAYTO,
                            LAST_NAME_ORG_PROVIDER_PAYTO,
                            FIRST_NAME_PROVIDER_PAYTO,
                            MIDDLE_NAME_PROVIDER_PAYTO,
                            NAME_PREFIX_PROVIDER_PAYTO,
                            NAME_SUFFIX_PROVIDER_PAYTO,
                            ID_CODE_QUALIFIER_PROVIDER_PAYTO,
                            ID_CODE_PROVIDER_PAYTO
                        )
        )
        , provider_payto_n3 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                                    when    flattened.index = 2   then      'ADDRESS_LINE_1_PROVIDER_PAYTO'
                                    when    flattened.index = 3   then      'ADDRESS_LINE_2_PROVIDER_PAYTO'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N3.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*87'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PROVIDER_PAYTO',
                                'ADDRESS_LINE_1_PROVIDER_PAYTO',
                                'ADDRESS_LINE_2_PROVIDER_PAYTO'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PROVIDER_PAYTO,
                            ADDRESS_LINE_1_PROVIDER_PAYTO,
                            ADDRESS_LINE_2_PROVIDER_PAYTO
                        )
        )
        , provider_payto_n4 as
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

                            case    when    flattened.index = 1   then      'ADDRESS_CODE_PROVIDER_PAYTO'
                                    when    flattened.index = 2   then      'CITY_PROVIDER_PAYTO'
                                    when    flattened.index = 3   then      'ST_PROVIDER_PAYTO'
                                    when    flattened.index = 4   then      'ZIP_PROVIDER_PAYTO'
                                    when    flattened.index = 5   then      'COUNTRY_PROVIDER_PAYTO'
                                    when    flattened.index = 6   then      'LOCATION_QUALIFIER_PROVIDER_PAYTO'
                                    when    flattened.index = 7   then      'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                                    end     as value_header,

                            nullif(trim(flattened.value), '') as value_format

                from        filtered_hl,
                            lateral split_to_table(filtered_hl.line_element_837, '*') as flattened            --2 Flatten

                where       regexp_like(filtered_hl.line_element_837, '^N4.*')                          --1 Filter
                            and filtered_hl.lag_name_indicator = 'NM1*87'
            )
            select      *
            from        long
                        pivot(
                            max(value_format) for value_header in (
                                'ADDRESS_CODE_PROVIDER_PAYTO',
                                'CITY_PROVIDER_PAYTO',
                                'ST_PROVIDER_PAYTO',
                                'ZIP_PROVIDER_PAYTO',
                                'COUNTRY_PROVIDER_PAYTO',
                                'LOCATION_QUALIFIER_PROVIDER_PAYTO',
                                'LOCATION_IDENTIFIER_PROVIDER_PAYTO'
                            )
                        )   as pvt (
                            ID_837,
                            NTH_TRANSACTION_SET,
                            INDEX,
                            HL_INDEX_CURRENT,
                            HL_INDEX_BILLING_20,
                            HL_INDEX_SUBSCRIBER_22,
                            HL_INDEX_PATIENT_23,
                            ADDRESS_CODE_PROVIDER_PAYTO,
                            CITY_PROVIDER_PAYTO,
                            ST_PROVIDER_PAYTO,
                            ZIP_PROVIDER_PAYTO,
                            COUNTRY_PROVIDER_PAYTO,
                            LOCATION_QUALIFIER_PROVIDER_PAYTO,
                            LOCATION_IDENTIFIER_PROVIDER_PAYTO
                        )
        )
        select      header.id_837,
                    header.nth_transaction_set,
                    header.index,
                    header.hl_index_current,
                    header.hl_index_billing_20,
                    header.hl_index_subscriber_22,
                    header.hl_index_patient_23,
                    header.hl_prefix_provider,
                    header.hl_id_provider,
                    header.hl_parent_id_provider,
                    header.hl_level_code_provider,
                    header.hl_child_code_provider,
                    prv.prv_prefix_provider,
                    prv.provider_code_provider,
                    prv.reference_id_qualifier_provider,
                    prv.provider_taxonomy_code_provider,
                    nm85.name_code_provider,
                    nm85.entity_identifier_code_provider,
                    nm85.entity_type_qualifier_provider,
                    nm85.last_name_org_provider,
                    nm85.first_name_provider,
                    nm85.middle_name_provider,
                    nm85.name_prefix_provider,
                    nm85.name_suffix_provider,
                    nm85.id_code_qualifier_provider,
                    nm85.id_code_provider,
                    n3.address_code_provider,
                    n3.address_line_1_provider,
                    n3.address_line_2_provider,
                    n4.address_code_provider,
                    n4.city_provider,
                    n4.st_provider,
                    n4.zip_provider,
                    n4.country_provider,
                    n4.location_qualifier_provider,
                    n4.location_identifier_provider,
                    ref.ref_code_provider,
                    ref.reference_id_qualifier_provider,
                    ref.reference_id_provider,
                    ref.description_provider,
                    per.provider_contact_prefix,
                    per.contact_function_code_provider,
                    per.contact_name_provider,
                    per.communication_qualifier_1_provider,
                    per.communication_number_1_provider,
                    per.communication_qualifier_2_provider,
                    per.communication_number_2_provider,
                    per.communication_qualifier_3_provider,
                    per.communication_number_3_provider,
                    payto_nm87.name_code_provider_payto,
                    payto_nm87.entity_identifier_code_provider_payto,
                    payto_nm87.entity_type_qualifier_provider_payto,
                    payto_nm87.last_name_org_provider_payto,
                    payto_nm87.first_name_provider_payto,
                    payto_nm87.middle_name_provider_payto,
                    payto_nm87.name_prefix_provider_payto,
                    payto_nm87.name_suffix_provider_payto,
                    payto_nm87.id_code_qualifier_provider_payto,
                    payto_nm87.id_code_provider_payto,
                    payto_n3.address_code_provider_payto,
                    payto_n3.address_line_1_provider_payto,
                    payto_n3.address_line_2_provider_payto,
                    payto_n4.address_code_provider_payto,
                    payto_n4.city_provider_payto,
                    payto_n4.st_provider_payto,
                    payto_n4.zip_provider_payto,
                    payto_n4.country_provider_payto,
                    payto_n4.location_qualifier_provider_payto,
                    payto_n4.location_identifier_provider_payto

        from        provider_hl         as header
                    left join
                        provider_prv        as prv
                        on  header.id_837               = prv.id_837
                        and header.nth_transaction_set  = prv.nth_transaction_set
                        and header.hl_index_billing_20  = prv.hl_index_billing_20
                    left join
                        provider_nm85       as nm85
                        on  header.id_837               = nm85.id_837
                        and header.nth_transaction_set  = nm85.nth_transaction_set
                        and header.hl_index_billing_20  = nm85.hl_index_billing_20
                    left join
                        provider_n3         as n3
                        on  header.id_837               = n3.id_837
                        and header.nth_transaction_set  = n3.nth_transaction_set
                        and header.hl_index_billing_20  = n3.hl_index_billing_20
                    left join
                        provider_n4         as n4
                        on  header.id_837               = n4.id_837
                        and header.nth_transaction_set  = n4.nth_transaction_set
                        and header.hl_index_billing_20  = n4.hl_index_billing_20
                    left join
                        provider_ref        as ref
                        on  header.id_837               = ref.id_837
                        and header.nth_transaction_set  = ref.nth_transaction_set
                        and header.hl_index_billing_20  = ref.hl_index_billing_20
                    left join
                        provider_per        as per
                        on  header.id_837               = per.id_837
                        and header.nth_transaction_set  = per.nth_transaction_set
                        and header.hl_index_billing_20  = per.hl_index_billing_20
                    left join
                        provider_payto_nm87 as payto_nm87
                        on  header.id_837               = payto_nm87.id_837
                        and header.nth_transaction_set  = payto_nm87.nth_transaction_set
                        and header.hl_index_billing_20  = payto_nm87.hl_index_billing_20
                    left join
                        provider_payto_n3   as payto_n3
                        on  header.id_837               = payto_n3.id_837
                        and header.nth_transaction_set  = payto_n3.nth_transaction_set
                        and header.hl_index_billing_20  = payto_n3.hl_index_billing_20
                    left join
                        provider_payto_n4   as payto_n4
                        on  header.id_837               = payto_n4.id_837
                        and header.nth_transaction_set  = payto_n4.nth_transaction_set
                        and header.hl_index_billing_20  = payto_n4.hl_index_billing_20
    )
    , subscriber_hl22 as --JOIN COMPLETED
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
    , patient_hl23 as --JOIN COMPLETED
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
    , claim_clm as --JOIN COMPLETED
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
    , clm_othersbr as --JOIN COMPLETED
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
    , clm_service_line_lx as --JOIN COMPLETED
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
    from        receiver_nm40
)
select      *
from        parse_transaction_sets
order by    1,2,3
;