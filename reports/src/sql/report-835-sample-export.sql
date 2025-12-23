select      
            transaction_set.transaction_set_control_number_header   as recordID,
            transaction_set.trans_amount                            as paymentAmount,
            transaction_set.payment_method_code                     as paymentMethod,
            transaction_set.payment_effective_date                  as dateOfPayment,
            transaction_set.trace_id                                as traceNumber,
            transaction_set.n1_payer_organization_name              as payerName,

            remits.clp_claim_id                                     as patientControlNumber,
            remits.clp_claim_charge_amount                          as totalClaimChargeAmount,
            remits.clp_claim_payment_amount                         as claimPaymentAmount,
            remits.clp_claim_patient_resp_amount                    as patientResponsibilityAmount,
            
            remits.nm1_qc_patient_last_name_org                     as patientLastName,
            remits.nm1_qc_patient_first_name                        as patientFirstName,
            remits.nm1_qc_patient_middle_name                       as patientMiddleName,
            remits.nm1_qc_patient_id_code                           as patientIdentifier,

            NULL                                                    as subscriberFirstName,
            NULL                                                    as subscriberIdentifier,

            NULL                                                    as correctedPatientInsuredLastName,
            NULL                                                    as correctedPatientInsuredFirstName,

            NULL                                                    as claimPaymentRemarkCode6,
            remits_service_lines.cas_adj_group_code                 as claimAdjustmentGroupCodeA,
            remits_service_lines.cas_reason_code_1                  as claimAdjustmentReasonCodeA1, --CARC
            remits_service_lines.cas_adj_amount_1                   as claimAdjustmentAmountA1,
            remits_service_lines.cas_reason_code_2                  as claimAdjustmentReasonCodeA2,
            remits_service_lines.cas_adj_amount_2                   as claimAdjustmentAmountA2,
            NULL                                                    as reasonCodeExpanded

from        edwprodhh.edi_835_parser.transaction_sets as transaction_set
            left join
                edwprodhh.edi_835_parser.remits as remits
                on  transaction_set.response_id             = remits.response_id
                and transaction_set.nth_functional_group    = remits.nth_functional_group
                and transaction_set.nth_transaction_set     = remits.nth_transaction_set
            left join
                edwprodhh.edi_835_parser.remits_service_lines as remits_service_lines --currently this is luckily a 1-1 join but is actually a 1-M join.
                on  remits.response_id                      = remits_service_lines.response_id
                and remits.nth_functional_group             = remits_service_lines.nth_functional_group
                and remits.nth_transaction_set              = remits_service_lines.nth_transaction_set
                and remits.lx_index                         = remits_service_lines.lx_index
order by    recordID
;