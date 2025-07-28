-- Sample UB files: T:\Complex Claims\IU\PATIENT FOLDERS

-- create or replace view
--     edwprodhh.edi_837_parser.export_ub
-- as
with last_response as
(
    with file_dates as
    (
        select      distinct
                    response_id,
                    file_date
        from        edwprodhh.edi_837_parser.response_flat
    )
    select      claims.claim_id,
                claims.response_id
    from        edwprodhh.edi_837_parser.claims as claims
                inner join
                    file_dates
                    on claims.response_id = file_dates.response_id
    qualify     row_number() over (partition by claims.claim_id order by file_dates.file_date desc) = 1
)
, claim_othersbr_b as
(
    select      *
    from        edwprodhh.edi_837_parser.claim_additional_subscribers
    qualify     row_number() over (partition by response_id, nth_transaction_set, claim_index order by index asc) = 1
)
, claim_othersbr_c as
(
    select      *
    from        edwprodhh.edi_837_parser.claim_additional_subscribers
    qualify     row_number() over (partition by response_id, nth_transaction_set, claim_index order by index asc) = 2
)
, joined as
(
    select      claims.response_id,
                claims.nth_transaction_set,
                claims.claim_id,
                lx.lx_assigned_line_number,


                --  PROVIDER INFORMATION
                hl_providers.last_name_org_provider                                                                             as provider_name_1a,
                coalesce(hl_providers.address_line_1_provider, '') ||
                    coalesce(hl_providers.address_line_2_provider, '')                                                          as provider_address_1b,
                nullif(regexp_replace(
                    coalesce(hl_providers.city_provider, '') || ', ' ||
                    coalesce(hl_providers.st_provider, '') || ' ' ||
                    coalesce(hl_providers.zip_provider, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as provider_address_1c,
                hl_providers.communication_number_1_provider                                                                    as provider_phone_1d,


                --  PAY-TO INFORMATION
                coalesce(hl_providers.last_name_org_provider_payto, hl_providers.last_name_org_provider, '')                    as payto_name_2a,
                coalesce(hl_providers.address_line_1_provider_payto, '') ||
                    coalesce(hl_providers.address_line_2_provider_payto, '')                                                    as payto_address_2b,
                nullif(regexp_replace(
                    coalesce(hl_providers.city_provider_payto, '') || ', ' ||
                    coalesce(hl_providers.st_provider_payto, '') || ' ' ||
                    coalesce(hl_providers.zip_provider_payto, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as payto_address_2c,
                NULL                                                                                                            as payto_phone_2d,                  --On UB, not in 837.


                --  RECORD IDS
                claims.claim_id                                                                                                 as patient_control_num_3a,
                claims.clm_ref_medical_record_num                                                                               as medical_record_num_3b,


                --  STATEMENT SUMMARIES
                NULL                                                                                                            as type_of_bill_4,                  --On UB, not in 837.

                insert(hl_providers.reference_id_provider, 3, 0, '-')                                                           as provider_fed_tax_num_5,
                to_varchar(claims.start_date_claim::date,   'MMDDYY')                                                           as statement_date_from_6a,
                to_varchar(claims.end_date_claim::date,     'MMDDYY')                                                           as statement_date_to_6b,
                NULL                                                                                                            as empty_7,


                --  PATIENT INFORMATION
                nullif(regexp_replace(
                    case    when    hl_patients.response_id is not null
                            then    coalesce(hl_patients.last_name_org_patient, '') || ', ' ||
                                        coalesce(hl_patients.first_name_patient, '') || ' ' ||
                                        coalesce(hl_patients.middle_name_patient, '')
                            else    coalesce(hl_subscribers.last_name_org_subscriber, '') || ', ' ||
                                        coalesce(hl_subscribers.first_name_subscriber, '') || ' ' ||
                                        coalesce(hl_subscribers.middle_name_subscriber, '')
                            end
                , '\\s*\\,?\\s*$'), '')                                                                                         as patient_name_8b,

                nullif(regexp_replace(
                    case    when    hl_patients.response_id is not null
                            then    coalesce(hl_patients.address_line_1_patient, '') ||
                                        coalesce(hl_patients.address_line_2_patient, '')
                            else    coalesce(hl_subscribers.address_line_1_subscriber, '') ||
                                        coalesce(hl_subscribers.address_line_2_subscriber, '')
                            end
                , '\\s*\\,?\\s*$'), '')                                                                                         as patient_address_9a,

                case    when    hl_patients.response_id is not null
                        then    hl_patients.city_patient
                        else    hl_subscribers.city_subscriber
                        end                                                                                                     as patient_city_9b,
                
                case    when    hl_patients.response_id is not null
                        then    hl_patients.st_patient
                        else    hl_subscribers.st_subscriber
                        end                                                                                                     as patient_state_9c,
                
                case    when    hl_patients.response_id is not null
                        then    hl_patients.zip_patient
                        else    hl_subscribers.zip_subscriber
                        end                                                                                                     as patient_zip_9d,

                case    when    hl_patients.response_id is not null
                        then    case    when    regexp_like(hl_patients.dob_patient, '^\\d{4}\\-\\d{2}\\-\\d{2}$')
                                        then    to_varchar(hl_patients.dob_patient::date, 'MMDDYYYY')
                                        else    NULL
                                        end
                        else    case    when    regexp_like(hl_subscribers.dob_subscriber, '^\\d{4}\\-\\d{2}\\-\\d{2}$')
                                        then    to_varchar(hl_subscribers.dob_subscriber::date, 'MMDDYYYY')
                                        else    NULL
                                        end
                        end                                                                                                     as patient_birthdate_10,

                case    when    hl_patients.response_id is not null
                        then    hl_patients.gender_code_patient
                        else    hl_subscribers.gender_code_subscriber
                        end                                                                                                     as patient_sex_11,


                --  ADMISSIONS, CONDITIONS, OCCURRENCES
                to_varchar(claims.admit_date_claim, 'MMDDYY')                                                                   as admission_date_12,
                claims.admit_hour_claim                                                                                         as admission_hr_13,
                claims.admission_type_code                                                                                      as admission_type_14,
                claims.admission_source_code                                                                                    as admission_src_15,
                left(claims.time_claim_time, 2)                                                                                 as dhr_16,
                claims.patient_status_code                                                                                      as status_code_17,


                filter(claims.clm_hi_array, x -> regexp_like(x, '^ABJ.*'))                                                      as abj,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^DR.*'))                                                       as dr,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^ABN.*'))                                                      as abn,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^APR.*'))                                                      as apr,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^BBR.*'))                                                      as bbr,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^BBQ.*'))                                                      as bbq,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^BH.*'))                                                       as bh,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^BG.*'))                                                       as bg,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^BE.*'))                                                       as be,
                filter(claims.clm_hi_array, x -> regexp_like(x, '^(ABK|ABF).*'))                                                as abk_abf,
                
                split(bg[0],        ':')[1]::varchar                                                                            as condition_codes_18,
                split(bg[1],        ':')[1]::varchar                                                                            as condition_codes_19,
                split(bg[2],        ':')[1]::varchar                                                                            as condition_codes_20,
                split(bg[3],        ':')[1]::varchar                                                                            as condition_codes_21,
                split(bg[4],        ':')[1]::varchar                                                                            as condition_codes_22,
                split(bg[5],        ':')[1]::varchar                                                                            as condition_codes_23,
                split(bg[6],        ':')[1]::varchar                                                                            as condition_codes_24,
                split(bg[7],        ':')[1]::varchar                                                                            as condition_codes_25,
                split(bg[8],        ':')[1]::varchar                                                                            as condition_codes_26,
                split(bg[9],        ':')[1]::varchar                                                                            as condition_codes_27,
                split(bg[10],       ':')[1]::varchar                                                                            as condition_codes_28,
                
                NULL                                                                                                            as acdt_state_29,
                NULL                                                                                                            as empty_30,

                split(bh[0],        ':')[1]::varchar                                                                            as occurrence_code_31,
                to_varchar(to_date(split(bh[0],     ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as occurrence_date_31,
                split(bh[1],        ':')[1]::varchar                                                                            as occurrence_code_32,
                to_varchar(to_date(split(bh[1],     ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as occurrence_date_32,
                split(bh[2],        ':')[1]::varchar                                                                            as occurrence_code_33,
                to_varchar(to_date(split(bh[2],     ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as occurrence_date_33,
                split(bh[3],        ':')[1]::varchar                                                                            as occurrence_code_34,
                to_varchar(to_date(split(bh[3],     ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as occurrence_date_34,
                
                NULL                                                                                                            as occurrence_span_35,              --On neither UB nor 837.
                NULL                                                                                                            as occurrence_span_36,              --On neither UB nor 837.
                NULL                                                                                                            as empty_37,                        --On neither UB nor 837.


                --  PAYER INFORMATION
                nullif(regexp_replace(
                    coalesce(hl_subscribers.last_name_org_subscriber_payor, '') || '\n' ||
                    coalesce(hl_subscribers.address_line_1_subscriber_payor, '') ||
                        coalesce(hl_subscribers.address_line_2_subscriber_payor, '') || '\n' ||

                    coalesce(hl_subscribers.city_subscriber_payor, '') || ', ' ||
                    coalesce(hl_subscribers.st_subscriber_payor, '') || ' ' ||
                    coalesce(hl_subscribers.zip_subscriber_payor, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as payor_38a,                       --Always on UB, sometimes found on some 837s.
                

                --  PAYER ADJUSTMENTS
                split(be[0],        ':')[1]::varchar                                                                            as value_codes_code_39a,
                split(be[0],        ':')[4]::varchar                                                                            as value_codes_amount_39a,
                split(be[1],        ':')[1]::varchar                                                                            as value_codes_code_40a,
                split(be[1],        ':')[4]::varchar                                                                            as value_codes_amount_40a,
                split(be[2],        ':')[1]::varchar                                                                            as value_codes_code_41a,
                split(be[2],        ':')[4]::varchar                                                                            as value_codes_amount_41a,
                split(be[3],        ':')[1]::varchar                                                                            as value_codes_code_39b,
                split(be[3],        ':')[4]::varchar                                                                            as value_codes_amount_39b,
                split(be[4],        ':')[1]::varchar                                                                            as value_codes_code_40b,
                split(be[4],        ':')[4]::varchar                                                                            as value_codes_amount_40b,
                split(be[5],        ':')[1]::varchar                                                                            as value_codes_code_41b,
                split(be[5],        ':')[4]::varchar                                                                            as value_codes_amount_41b,
                split(be[6],        ':')[1]::varchar                                                                            as value_codes_code_39c,
                split(be[6],        ':')[4]::varchar                                                                            as value_codes_amount_39c,
                split(be[7],        ':')[1]::varchar                                                                            as value_codes_code_40c,
                split(be[7],        ':')[4]::varchar                                                                            as value_codes_amount_40c,
                split(be[8],        ':')[1]::varchar                                                                            as value_codes_code_41c,
                split(be[8],        ':')[4]::varchar                                                                            as value_codes_amount_41c,
                split(be[9],        ':')[1]::varchar                                                                            as value_codes_code_39d,
                split(be[9],        ':')[4]::varchar                                                                            as value_codes_amount_39d,
                split(be[10],       ':')[1]::varchar                                                                            as value_codes_code_40d,
                split(be[10],       ':')[4]::varchar                                                                            as value_codes_amount_40d,
                split(be[11],       ':')[1]::varchar                                                                            as value_codes_code_41d,
                split(be[11],       ':')[4]::varchar                                                                            as value_codes_amount_41d,
                
                
                --  PAYER ARRAY
                --  PAYER A
                hl_subscribers.last_name_org_subscriber_payor                                                                   as payer_name_50a,
                hl_subscribers.id_code_subscriber_payor                                                                         as health_plan_id_51a,              --On neither UB nor 837.
                claims.release_of_info_code                                                                                     as release_of_info_52a,
                claims.benefits_assignment_indicator                                                                            as benefits_assignment_53a,
                NULL                                                                                                            as prior_payments_54a,              --On neither UB nor 837.
                NULL                                                                                                            as est_amount_due_55a,              --On neither UB nor 837.

                --  PAYER B
                claim_othersbr_b.last_name_org_otherpyr                                                                         as payer_name_50b,
                claim_othersbr_b.id_code_othersbr_nmpr                                                                          as health_plan_id_51b,
                claim_othersbr_b.release_of_info_othersbr                                                                       as release_of_info_52b,
                claim_othersbr_b.benefits_assignment_othersbr                                                                   as benefits_assignment_53b,
                NULL                                                                                                            as prior_payments_54b,
                NULL                                                                                                            as est_amount_due_55b,

                --  PAYER C
                claim_othersbr_c.last_name_org_otherpyr                                                                         as payer_name_50c,
                claim_othersbr_c.id_code_othersbr_nmpr                                                                          as health_plan_id_51c,
                claim_othersbr_c.release_of_info_othersbr                                                                       as release_of_info_52c,
                claim_othersbr_c.benefits_assignment_othersbr                                                                   as benefits_assignment_53c,
                NULL                                                                                                            as prior_payments_54c,
                NULL                                                                                                            as est_amount_due_55c,


                hl_providers.id_code_provider                                                                                   as npi_56,
                NULL                                                                                                            as empty_57,                        --On neither UB nor 837.
                
                
                --  POLICYHOLDER ARRAY
                --  POLICYHOLDER A
                nullif(regexp_replace(
                    coalesce(hl_subscribers.last_name_org_subscriber, '') || ', ' ||
                        coalesce(hl_subscribers.first_name_subscriber, '') || ' ' ||
                        coalesce(hl_subscribers.middle_name_subscriber, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as insured_name_58a,
                hl_subscribers.individual_relationship_code_subscriber                                                          as p_relationship_59a,
                hl_subscribers.id_code_subscriber                                                                               as insured_unique_id_60a,
                hl_subscribers.group_name_subscriber                                                                            as insured_group_name_61a,
                hl_subscribers.group_number_subscriber                                                                          as insurance_group_number_62a,

                --  POLICYHOLDER B
                nullif(regexp_replace(
                    coalesce(claim_othersbr_b.last_name_org_othersbr, '') || ', ' ||
                        coalesce(claim_othersbr_b.first_name_othersbr, '') || ' ' ||
                        coalesce(claim_othersbr_b.middle_name_othersbr, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as insured_name_58b,
                claim_othersbr_b.individual_relationship_code_othersbr                                                          as p_relationship_59b,
                claim_othersbr_b.id_code_othersbr_nmil                                                                          as insured_unique_id_60b,
                claim_othersbr_b.group_name_othersbr                                                                            as insured_group_name_61b,
                claim_othersbr_b.group_number_othersbr                                                                          as insurance_group_number_62b,

                --  POLICYHOLDER C
                nullif(regexp_replace(
                    coalesce(claim_othersbr_c.last_name_org_othersbr, '') || ', ' ||
                        coalesce(claim_othersbr_c.first_name_othersbr, '') || ' ' ||
                        coalesce(claim_othersbr_c.middle_name_othersbr, '')
                , '\\s*\\,?\\s*$'), '')                                                                                         as insured_name_58c,
                claim_othersbr_c.individual_relationship_code_othersbr                                                          as p_relationship_59c,
                claim_othersbr_c.id_code_othersbr_nmil                                                                          as insured_unique_id_60c,
                claim_othersbr_c.group_name_othersbr                                                                            as insured_group_name_61c,
                claim_othersbr_c.group_number_othersbr                                                                          as insurance_group_number_62c,
                

                --  TREATMENT AUTH CODES
                clm_ref_treatment_auth_codes_array[0]::varchar                                                                  as treatment_auth_codes_63a,
                clm_ref_treatment_auth_codes_array[1]::varchar                                                                  as treatment_auth_codes_63b,
                clm_ref_treatment_auth_codes_array[2]::varchar                                                                  as treatment_auth_codes_63c,
                NULL                                                                                                            as document_control_num_64,         --On neither UB nor 837.
                NULL                                                                                                            as employer_name_65,                --On UB, not in 837.
                
                
                --  DIAGNOSIS ARRAY & PROCEDURE CODES
                split(abk_abf[0],   ':')[1]::varchar                                                                            as dx_66_0A,
                split(abk_abf[0],   ':')[8]::varchar                                                                            as dx_66_0B,
                split(abk_abf[1],   ':')[1]::varchar                                                                            as dx_66_1A,
                split(abk_abf[1],   ':')[8]::varchar                                                                            as dx_66_1B,
                split(abk_abf[2],   ':')[1]::varchar                                                                            as dx_66_2A,
                split(abk_abf[2],   ':')[8]::varchar                                                                            as dx_66_2B,
                split(abk_abf[3],   ':')[1]::varchar                                                                            as dx_66_3A,
                split(abk_abf[3],   ':')[8]::varchar                                                                            as dx_66_3B,
                split(abk_abf[4],   ':')[1]::varchar                                                                            as dx_66_4A,
                split(abk_abf[4],   ':')[8]::varchar                                                                            as dx_66_4B,
                split(abk_abf[5],   ':')[1]::varchar                                                                            as dx_66_5A,
                split(abk_abf[5],   ':')[8]::varchar                                                                            as dx_66_5B,
                split(abk_abf[6],   ':')[1]::varchar                                                                            as dx_66_6A,
                split(abk_abf[6],   ':')[8]::varchar                                                                            as dx_66_6B,
                split(abk_abf[7],   ':')[1]::varchar                                                                            as dx_66_7A,
                split(abk_abf[7],   ':')[8]::varchar                                                                            as dx_66_7B,
                split(abk_abf[8],   ':')[1]::varchar                                                                            as dx_66_8A,
                split(abk_abf[8],   ':')[8]::varchar                                                                            as dx_66_8B,
                split(abk_abf[9],   ':')[1]::varchar                                                                            as dx_66_9A,
                split(abk_abf[9],   ':')[8]::varchar                                                                            as dx_66_9B,
                split(abk_abf[10],  ':')[1]::varchar                                                                            as dx_66_10A,
                split(abk_abf[10],  ':')[8]::varchar                                                                            as dx_66_10B,
                split(abk_abf[11],  ':')[1]::varchar                                                                            as dx_66_11A,
                split(abk_abf[11],  ':')[8]::varchar                                                                            as dx_66_11B,
                split(abk_abf[12],  ':')[1]::varchar                                                                            as dx_66_21A,
                split(abk_abf[12],  ':')[8]::varchar                                                                            as dx_66_21B,
                split(abk_abf[13],  ':')[1]::varchar                                                                            as dx_66_3A1,
                split(abk_abf[13],  ':')[8]::varchar                                                                            as dx_66_3B1,
                split(abk_abf[14],  ':')[1]::varchar                                                                            as dx_66_4A1,
                split(abk_abf[14],  ':')[8]::varchar                                                                            as dx_66_4B1,
                split(abk_abf[15],  ':')[1]::varchar                                                                            as dx_66_5A1,
                split(abk_abf[15],  ':')[8]::varchar                                                                            as dx_66_5B1,
                split(abk_abf[16],  ':')[1]::varchar                                                                            as dx_66_6A1,
                split(abk_abf[16],  ':')[8]::varchar                                                                            as dx_66_6B1,
                split(abk_abf[17],  ':')[1]::varchar                                                                            as dx_66_17A,
                split(abk_abf[17],  ':')[8]::varchar                                                                            as dx_66_17B,

                split(abj[0],       ':')[1]::varchar                                                                            as admit_dx_69,
                
                split(apr[0],       ':')[1]::varchar                                                                            as patient_reason_dx_70a,
                split(apr[1],       ':')[1]::varchar                                                                            as patient_reason_dx_70b,
                split(apr[2],       ':')[1]::varchar                                                                            as patient_reason_dx_70c,

                split(dr[0],        ':')[1]::varchar                                                                            as pps_code_71,
                split(abn[0],       ':')[1]::varchar                                                                            as ec_72a,
                split(abn[1],       ':')[1]::varchar                                                                            as ec_72b,
                split(abn[2],       ':')[1]::varchar                                                                            as ec_72c,
                NULL                                                                                                            as empty_73,                        --On neither UB nor 837.

                split(bbr[0],       ':')[1]::varchar                                                                            as prin_proc_code_74,
                to_varchar(to_date(split(bbr[0],    ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as prin_proc_date_74,
                split(bbq[0],       ':')[1]::varchar                                                                            as other_proc_code_74a,
                to_varchar(to_date(split(bbq[0],    ':')[3]::varchar, 'YYYYMMDD'), 'MMDDYY')                                    as other_proc_date_74a,
                NULL                                                                                                            as other_proc_code_74b,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_date_74b,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_code_74c,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_date_74c,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_code_74d,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_date_74d,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_code_74e,             --On neither UB nor 837.
                NULL                                                                                                            as other_proc_date_74e,             --On neither UB nor 837.
                NULL                                                                                                            as empty_75,                        --On neither UB nor 837.
                
                
                --  PHYSICIAN INFORMATION
                claims.id_code_attending                                                                                        as attending_npi_76a,
                claims.entity_identifier_code_attending                                                                         as attending_qualifier_76b,
                claims.last_name_org_attending                                                                                  as attending_last_name_76c,
                claims.first_name_attending                                                                                     as attending_first_name_76d,

                claims.id_code_operating                                                                                        as operating_npi_77a,
                claims.entity_identifier_code_operating                                                                         as operating_qualifier_77b,
                claims.last_name_org_operating                                                                                  as operating_last_name_77c,
                claims.first_name_operating                                                                                     as operating_first_name_77d,

                NULL                                                                                                            as other1_npi_78a,                  --On neither UB nor 837.
                NULL                                                                                                            as other1_last_name_78c,            --On neither UB nor 837.
                NULL                                                                                                            as other1_first_name_78d,           --On neither UB nor 837.

                NULL                                                                                                            as other2_npi_79a,                  --On neither UB nor 837.
                NULL                                                                                                            as other2_last_name_79c,            --On neither UB nor 837.
                NULL                                                                                                            as other2_first_name_79d,           --On neither UB nor 837.
                
                
                --  FOOTER
                NULL                                                                                                            as remarks_80,                      --On neither UB nor 837.
                hl_providers.provider_taxonomy_code_provider                                                                    as cc_81,                           --On UB, not in 837.

                
                --  CHARGES
                lx.revenue_code                                                                                                 as revenue_code_42,
                lx.description_lx                                                                                               as description_43,                  --On UB, not in 837.
                regexp_replace(regexp_substr(lx.procedure_code, '\\d+(\\:\\w*)?$'), ':', ' ')                                   as hipps_code_44,
                case    when    regexp_like(lx.date_lx, '^\\d{4}\\-\\d{2}\\-\\d{2}$')
                        then    to_varchar(lx.date_lx::date, 'MMDDYY')
                        else    NULL
                        end                                                                                                     as service_date_45,
                lx.service_units                                                                                                as service_units_46,
                lx.charge_amount                                                                                                as total_charges_47,
                claims.total_claim_charge                                                                                       as total_charges_47_sum,
                lx.sv2_mod_2                                                                                                    as non_covered_charges_48,
                NULL                                                                                                            as empty_49,

    from        edwprodhh.edi_837_parser.claims as claims
                inner join
                    last_response
                    on  claims.claim_id     = last_response.claim_id
                    and claims.response_id  = last_response.response_id

                left join
                    edwprodhh.edi_837_parser.hl_billing_providers as hl_providers
                    on  claims.response_id          = hl_providers.response_id
                    and claims.nth_transaction_set  = hl_providers.nth_transaction_set
                
                -- Do NOT use coalesce() on fields from below. If patient is present but some fields null, then could mix with Subscriber.
                left join
                    edwprodhh.edi_837_parser.hl_subscribers as hl_subscribers                   --effectively "claim_othersbr_a" for payer.
                    on  claims.response_id          = hl_subscribers.response_id
                    and claims.nth_transaction_set  = hl_subscribers.nth_transaction_set
                left join
                    edwprodhh.edi_837_parser.hl_patients as hl_patients                         --effectively "claim_othersbr_a" for policyholder.
                    on  claims.response_id          = hl_patients.response_id
                    and claims.nth_transaction_set  = hl_patients.nth_transaction_set

                --currently, potential 1:M join here. find an example with multiple and extract wide-ly.
                left join
                    claim_othersbr_b
                    on  claims.response_id          = claim_othersbr_b.response_id
                    and claims.nth_transaction_set  = claim_othersbr_b.nth_transaction_set
                    and claims.claim_index          = claim_othersbr_b.claim_index
                left join
                    claim_othersbr_c
                    on  claims.response_id          = claim_othersbr_c.response_id
                    and claims.nth_transaction_set  = claim_othersbr_c.nth_transaction_set
                    and claims.claim_index          = claim_othersbr_c.claim_index

                --intentional 1:M join here
                left join
                    edwprodhh.edi_837_parser.claim_service_lines as lx
                    on  claims.response_id          = lx.response_id
                    and claims.nth_transaction_set  = lx.nth_transaction_set
                    and claims.claim_index          = lx.claim_index

    where       claims.claim_id = '1236421390'
                    
    order by    1,2,3,4
)
select      *
from        joined
order by    response_id,
            nth_transaction_set,
            claim_id,
            lx_assigned_line_number::number
;