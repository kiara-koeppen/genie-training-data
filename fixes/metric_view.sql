-- Beat 5: Deploy the governed claims_metrics metric view
-- Apply this BEFORE Session 0 (pre-deploy so the instructor can simply add it
-- to the Space during Beat 5). Alternatively, run live during Beat 5.
--
-- Before: Genie builds denial rate / net denial rate SQL from scratch each
--   time, which is fragile and easy to get wrong — especially Net Denial Rate,
--   which requires nested filters across initial_decision + appeal_decision.
-- After: Genie calls MEASURE(`Denial Rate`) / MEASURE(`Net Denial Rate`) etc.
--   directly. Every consumer (Genie, dashboards, alerts) gets the same answer.

CREATE OR REPLACE VIEW genie_training.claims_analytics.claims_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Governed claims analytics metrics. Standard measures for denial rates, first-pass rates, turnaround times, and payment analysis. Excludes voided and test claims globally. Excludes Pharmacy from denial-rate measures per Operations team definition."
  source: genie_training.claims_analytics.claims
  filter: voided_flag = 'N' AND test_flag = 'N'

  joins:
    - name: patients
      source: genie_training.claims_analytics.patients
      on: source.mbr_id = patients.patient_id
    - name: providers
      source: genie_training.claims_analytics.providers
      on: source.rendering_prov_id = providers.provider_id
    - name: facilities
      source: genie_training.claims_analytics.facilities
      on: source.svc_location_id = facilities.facility_id
    - name: payers
      source: genie_training.claims_analytics.payers
      on: source.insurance_id = payers.payer_id

  dimensions:
    - name: Claim Type
      expr: source.claim_type
      comment: "Professional, Facility, or Pharmacy"
    - name: Service Line
      expr: source.service_line_code
      comment: "Clinical service category"
    - name: Initial Decision
      expr: source.initial_decision
      comment: "First adjudication outcome: APPROVED, DENIED, PENDING, PARTIAL"
    - name: Appeal Decision
      expr: source.appeal_decision
      comment: "Appeal outcome if filed: OVERTURNED, UPHELD, or NULL (no appeal)"
    - name: Receipt Month
      expr: DATE_TRUNC('MONTH', source.receipt_date)
      comment: "Month the claim was received"
    - name: Payer Name
      expr: payers.payer_name
      comment: "Insurance payer: BCBSU, UnitedHealthcare, Medicare, etc."
    - name: Facility Name
      expr: facilities.facility_name
      comment: "Facility where service was rendered"
    - name: Facility State
      expr: facilities.state_code
      comment: "Two-letter state code of the facility"
    - name: Provider Specialty
      expr: providers.provider_specialty
      comment: "Medical specialty of the rendering provider"
    - name: Patient Age Group
      expr: patients.patient_age_group
      comment: "Age band: 0-17, 18-34, 35-49, 50-64, 65+"
    - name: Patient State
      expr: patients.patient_state
      comment: "State of patient residence"

  measures:
    - name: Total Claims
      expr: COUNT(1)
      comment: "Total number of claims (excluding voided and test)"
    - name: Denial Rate
      expr: COUNT(1) FILTER (WHERE source.initial_decision = 'DENIED' AND source.claim_type IN ('Professional', 'Facility')) * 100.0 / NULLIF(COUNT(1) FILTER (WHERE source.claim_type IN ('Professional', 'Facility')), 0)
      comment: "Governed denial rate for Professional and Facility claims only. Excludes Pharmacy per Operations team definition. Expected ~8.2%."
    - name: Net Denial Rate
      expr: COUNT(1) FILTER (WHERE source.initial_decision = 'DENIED' AND (source.appeal_decision IS NULL OR source.appeal_decision = 'UPHELD') AND source.claim_type IN ('Professional', 'Facility')) * 100.0 / NULLIF(COUNT(1) FILTER (WHERE source.claim_type IN ('Professional', 'Facility')), 0)
      comment: "Net denial rate — claims that were denied and stayed denied (appeal not filed, or appeal upheld). This is the metric Operations tracks for revenue impact. Excludes Pharmacy. Expected ~7.2%."
    - name: First Pass Rate
      expr: COUNT(1) FILTER (WHERE source.first_pass_rate = 'Y') * 100.0 / COUNT(1)
      comment: "Percentage of claims passing all edits on first submission. Expected ~94.2%."
    - name: Appeal Overturn Rate
      expr: COUNT(1) FILTER (WHERE source.appeal_decision = 'OVERTURNED') * 100.0 / NULLIF(COUNT(1) FILTER (WHERE source.appeal_decision IS NOT NULL), 0)
      comment: "Percentage of filed appeals that were overturned"
    - name: Average Turnaround Days
      expr: AVG(DATEDIFF(source.adjudication_date, source.receipt_date))
      comment: "Average days from claim receipt to adjudication"
    - name: Total Paid Amount
      expr: SUM(source.paid_amount)
      comment: "Total dollars paid across all claims"
    - name: Total Billed Amount
      expr: SUM(source.billed_amount)
      comment: "Total dollars billed by providers"
$$;
