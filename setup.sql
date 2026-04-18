-- Genie Space Training Data Setup Script — BARE STATE
-- Healthcare Claims Analytics
--
-- This script deploys the UNDERCONFIGURED version of the training data.
-- During Session 0's progressive demo, the instructor runs fix scripts
-- from /fixes/ to improve the Space one beat at a time.
--
-- What's intentionally missing (by design):
--   - No table or column descriptions (forces descriptions fix in Beat 1)
--   - No primary keys or foreign keys (forces join fix in Beat 2)
--   - Foreign key columns renamed to non-obvious healthcare shortnames
--     (mbr_id, rendering_prov_id, svc_location_id, insurance_id) so Genie
--     can't guess the join target from column name alone
--   - No metric view (added in Beat 5)
--
-- Data calibration (intentional skew for demo impact):
--   - Pharmacy: ~20% of claims at ~60% denial rate (was 10% / 40%)
--   - Prof + Facility: ~8.2% denial rate
--   - Naive unfiltered denial rate: ~18-19% ("woah this is way off")
--   - Governed denial rate (Prof + Facility only): ~8.2%
--   - Gap: ~10 percentage points, demonstrating metric view value
--
-- Prerequisites:
--   1. CREATE CATALOG IF NOT EXISTS genie_training;
--   2. CREATE SCHEMA IF NOT EXISTS genie_training.claims_analytics;
--   3. Replace 'genie_training' below if your catalog name is different.
--
-- Usage:
--   Run each statement one at a time in a Databricks SQL editor, or put
--   each statement in its own notebook cell.

-- ============================================================================
-- FACILITIES
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.facilities (
  facility_id INT NOT NULL,
  facility_name STRING,
  facility_type STRING,
  city STRING,
  state_code STRING
);

INSERT INTO genie_training.claims_analytics.facilities VALUES
(1, 'Intermountain Medical Center', 'Hospital', 'Murray', 'UT'),
(2, 'LDS Hospital', 'Hospital', 'Salt Lake City', 'UT'),
(3, 'Primary Childrens Medical Center', 'Hospital', 'Salt Lake City', 'UT'),
(4, 'McKay-Dee Hospital', 'Hospital', 'Ogden', 'UT'),
(5, 'Dixie Regional Medical Center', 'Hospital', 'St. George', 'UT'),
(6, 'Utah Valley Hospital', 'Hospital', 'Provo', 'UT'),
(7, 'Logan Regional Hospital', 'Hospital', 'Logan', 'UT'),
(8, 'Cassia Regional Hospital', 'Hospital', 'Burley', 'ID'),
(9, 'St. James Hospital', 'Hospital', 'Butte', 'MT'),
(10, 'Cedar City Hospital', 'Hospital', 'Cedar City', 'UT');

-- ============================================================================
-- PAYERS
-- Note: "BCBS of Utah" is stored as "BCBSU" with no "Blue Cross" substring
-- so Genie can't fuzzy-match "Blue Cross" without an explicit synonym.
-- "Regence BlueCross BlueShield" is kept as a distractor — Regence is the
-- Idaho/Oregon Blue Cross plan, NOT the Utah plan. When a user asks about
-- "Blue Cross" denials, Genie will likely pick Regence (wrong) unless the
-- synonym "Blue Cross" -> "BCBSU" is added to the Knowledge Store.
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.payers (
  payer_id INT NOT NULL,
  payer_name STRING,
  payer_type STRING
);

INSERT INTO genie_training.claims_analytics.payers VALUES
(1, 'BCBSU', 'Commercial'),
(2, 'UnitedHealthcare', 'Commercial'),
(3, 'Aetna', 'Commercial'),
(4, 'Cigna', 'Commercial'),
(5, 'Regence BlueCross BlueShield', 'Commercial'),
(6, 'Medicare', 'Medicare'),
(7, 'Medicaid', 'Medicaid'),
(8, 'SelectHealth', 'Commercial'),
(9, 'DMBA', 'Commercial'),
(10, 'Self-Pay', 'Self-Pay');

-- ============================================================================
-- PROVIDERS
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.providers (
  provider_id INT NOT NULL,
  provider_name STRING,
  provider_specialty STRING,
  provider_npi STRING
);

INSERT INTO genie_training.claims_analytics.providers VALUES
(1, 'Dr. Sarah Mitchell', 'Cardiovascular Services', '1234567890'),
(2, 'Dr. James Chen', 'Orthopedic Surgery', '1234567891'),
(3, 'Dr. Maria Rodriguez', 'Internal Medicine', '1234567892'),
(4, 'Dr. David Park', 'Emergency Medicine', '1234567893'),
(5, 'Dr. Lisa Thompson', 'Family Medicine', '1234567894'),
(6, 'Dr. Robert Kim', 'Neurology', '1234567895'),
(7, 'Dr. Jennifer Adams', 'Pediatrics', '1234567896'),
(8, 'Dr. Michael Brown', 'General Surgery', '1234567897'),
(9, 'Dr. Emily Watson', 'Oncology', '1234567898'),
(10, 'Dr. Andrew Lee', 'Pulmonology', '1234567899'),
(11, 'Dr. Rachel Green', 'Dermatology', '1234567900'),
(12, 'Dr. Thomas Wilson', 'Gastroenterology', '1234567901'),
(13, 'Dr. Amanda Foster', 'Obstetrics', '1234567902'),
(14, 'Dr. Kevin Patel', 'Urology', '1234567903'),
(15, 'Dr. Susan Chang', 'Endocrinology', '1234567904');

-- ============================================================================
-- PATIENTS (5,000 synthetic patients)
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.patients (
  patient_id INT NOT NULL,
  patient_age_group STRING,
  patient_state STRING,
  patient_gender STRING
);

INSERT INTO genie_training.claims_analytics.patients
SELECT
  id as patient_id,
  CASE
    WHEN hash(id) % 100 < 15 THEN '0-17'
    WHEN hash(id) % 100 < 35 THEN '18-34'
    WHEN hash(id) % 100 < 55 THEN '35-49'
    WHEN hash(id) % 100 < 80 THEN '50-64'
    ELSE '65+'
  END as patient_age_group,
  CASE
    WHEN abs(hash(id + 1000)) % 100 < 60 THEN 'UT'
    WHEN abs(hash(id + 1000)) % 100 < 72 THEN 'ID'
    WHEN abs(hash(id + 1000)) % 100 < 80 THEN 'NV'
    WHEN abs(hash(id + 1000)) % 100 < 87 THEN 'CO'
    WHEN abs(hash(id + 1000)) % 100 < 92 THEN 'MT'
    WHEN abs(hash(id + 1000)) % 100 < 95 THEN 'WY'
    WHEN abs(hash(id + 1000)) % 100 < 97 THEN 'KS'
    ELSE 'NE'
  END as patient_state,
  CASE WHEN abs(hash(id + 2000)) % 100 < 52 THEN 'F' ELSE 'M' END as patient_gender
FROM range(1, 5001) t(id);

-- ============================================================================
-- MEMBERS (insurance enrollment records — distractor table for Beat 2)
-- Represents insurance member records, which are distinct from clinical patient
-- records. Each patient has a corresponding member record with plan info.
-- member_id uses a MEM- prefix format so its values do NOT match any other
-- table by direct value, forcing Genie to need an explicit join path:
--   claims -> patients (via mbr_id = patient_id)
--   patients -> members (via patient_id = patient_id)
-- This is the Beat 2 failure moment: "denial rate by plan type" requires
-- going through patients to get to members, and Genie can't guess that path.
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.members (
  member_id STRING,
  patient_id INT,
  plan_type STRING,
  plan_tier STRING,
  enrollment_date DATE,
  effective_date DATE
);

INSERT INTO genie_training.claims_analytics.members
SELECT
  concat('MEM-', lpad(cast(id as string), 7, '0')) as member_id,
  id as patient_id,
  CASE
    WHEN abs(hash(id + 3000)) % 100 < 45 THEN 'Commercial'
    WHEN abs(hash(id + 3000)) % 100 < 70 THEN 'Medicare Advantage'
    WHEN abs(hash(id + 3000)) % 100 < 90 THEN 'Medicaid Managed Care'
    ELSE 'Self-Funded'
  END as plan_type,
  CASE
    WHEN abs(hash(id + 3000)) % 100 < 45 THEN
      CASE
        WHEN abs(hash(id + 3100)) % 100 < 20 THEN 'Bronze'
        WHEN abs(hash(id + 3100)) % 100 < 55 THEN 'Silver'
        WHEN abs(hash(id + 3100)) % 100 < 85 THEN 'Gold'
        ELSE 'Platinum'
      END
    ELSE NULL
  END as plan_tier,
  date_add('2020-01-01', cast(abs(hash(id + 3200)) % 1500 as int)) as enrollment_date,
  date_add('2024-01-01', cast(abs(hash(id + 3300)) % 365 as int)) as effective_date
FROM range(1, 5001) t(id);

-- ============================================================================
-- CLAIMS (50,000 synthetic claims)
-- FK columns renamed to non-obvious healthcare shortnames so Genie cannot
-- guess joins from column names alone:
--   patient_id    -> mbr_id               (member ID, common in healthcare)
--   provider_id   -> rendering_prov_id    (rendering provider)
--   facility_id   -> svc_location_id      (service location)
--   payer_id      -> insurance_id         (insurance plan)
--
-- Calibrated to produce:
--   Denial rate (Prof + Facility, excl voided/test): ~8.2% (governed)
--   Denial rate (all types, no exclusions):          ~18-19% (inflated, demo)
--   Pharmacy denial rate:                            ~60% (drives inflation)
--   Pharmacy share of total claims:                  ~20%
--   First-pass rate:                                 ~94.2%
--   Dates: 18 months of data from Jan 2025
-- ============================================================================

CREATE OR REPLACE TABLE genie_training.claims_analytics.claims (
  claim_id STRING,
  claim_type STRING,
  receipt_date DATE,
  adjudication_date DATE,
  initial_decision STRING,
  appeal_decision STRING,
  paid_amount DECIMAL(12,2),
  billed_amount DECIMAL(12,2),
  first_pass_rate STRING,
  voided_flag STRING,
  test_flag STRING,
  service_line_code STRING,
  mbr_id INT,
  rendering_prov_id INT,
  svc_location_id INT,
  insurance_id INT
);

INSERT INTO genie_training.claims_analytics.claims
SELECT
  concat('CLM-', lpad(cast(id as string), 7, '0')) as claim_id,
  CASE
    WHEN abs(hash(id)) % 100 < 40 THEN 'Professional'
    WHEN abs(hash(id)) % 100 < 80 THEN 'Facility'
    ELSE 'Pharmacy'
  END as claim_type,
  date_add('2025-01-01', cast(abs(hash(id + 100)) % 450 as int)) as receipt_date,
  CASE
    WHEN abs(hash(id + 200)) % 100 < 2 THEN NULL
    ELSE date_add(date_add('2025-01-01', cast(abs(hash(id + 100)) % 450 as int)), 3 + cast(abs(hash(id + 300)) % 19 as int))
  END as adjudication_date,
  -- initial_decision: Pharmacy claims get ~60% denial rate to create a
  -- dramatic contrast vs Professional/Facility at ~8.2%. This makes the
  -- unfiltered denial rate ~18-19%, demonstrating why metric view
  -- exclusions matter. Gap is ~10pt ("woah that's way off").
  CASE
    WHEN abs(hash(id + 200)) % 100 < 2 THEN 'PENDING'
    WHEN abs(hash(id)) % 100 >= 80 AND abs(hash(id + 400)) % 100 < 60 THEN 'DENIED'
    WHEN abs(hash(id)) % 100 < 80 AND abs(hash(id + 400)) % 1000 < 82 THEN 'DENIED'
    WHEN abs(hash(id + 400)) % 1000 < 112 THEN 'PARTIAL'
    ELSE 'APPROVED'
  END as initial_decision,
  CASE
    WHEN abs(hash(id)) % 100 >= 80 AND abs(hash(id + 400)) % 100 < 60 AND abs(hash(id + 500)) % 100 < 30 THEN
      CASE WHEN abs(hash(id + 600)) % 100 < 40 THEN 'OVERTURNED' ELSE 'UPHELD' END
    WHEN abs(hash(id)) % 100 < 80 AND abs(hash(id + 400)) % 1000 < 82 AND abs(hash(id + 500)) % 100 < 30 THEN
      CASE WHEN abs(hash(id + 600)) % 100 < 40 THEN 'OVERTURNED' ELSE 'UPHELD' END
    ELSE NULL
  END as appeal_decision,
  CASE
    WHEN abs(hash(id)) % 100 >= 80 AND abs(hash(id + 400)) % 100 < 60 THEN 0.00
    WHEN abs(hash(id)) % 100 < 80 AND abs(hash(id + 400)) % 1000 < 82 THEN 0.00
    WHEN abs(hash(id + 200)) % 100 < 2 THEN NULL
    ELSE round(200 + cast(abs(hash(id + 700)) % 4800 as decimal(12,2)), 2)
  END as paid_amount,
  -- billed_amount: long-tail distribution to support high-dollar claim analysis
  --   70% of claims: $300-$5,000  (standard/low)
  --   25% of claims: $5,000-$15,000 (upper middle)
  --    5% of claims: $15,000-$75,000 (high-dollar outliers, e.g., inpatient stays)
  CASE
    WHEN abs(hash(id + 800)) % 100 < 5 THEN round(15000 + cast(abs(hash(id + 850)) % 60000 as decimal(12,2)), 2)
    WHEN abs(hash(id + 800)) % 100 < 30 THEN round(5000 + cast(abs(hash(id + 850)) % 10000 as decimal(12,2)), 2)
    ELSE round(300 + cast(abs(hash(id + 850)) % 4700 as decimal(12,2)), 2)
  END as billed_amount,
  CASE WHEN abs(hash(id + 900)) % 1000 < 942 THEN 'Y' ELSE 'N' END as first_pass_rate,
  CASE WHEN abs(hash(id + 1000)) % 100 < 1 THEN 'Y' ELSE 'N' END as voided_flag,
  CASE WHEN abs(hash(id + 1100)) % 200 < 1 THEN 'Y' ELSE 'N' END as test_flag,
  CASE
    WHEN abs(hash(id + 1200)) % 100 < 20 THEN 'Cardiovascular Services'
    WHEN abs(hash(id + 1200)) % 100 < 35 THEN 'Orthopedics'
    WHEN abs(hash(id + 1200)) % 100 < 50 THEN 'Primary Care'
    WHEN abs(hash(id + 1200)) % 100 < 62 THEN 'Emergency Medicine'
    WHEN abs(hash(id + 1200)) % 100 < 72 THEN 'Oncology'
    WHEN abs(hash(id + 1200)) % 100 < 80 THEN 'Neurology'
    WHEN abs(hash(id + 1200)) % 100 < 87 THEN 'Pediatrics'
    WHEN abs(hash(id + 1200)) % 100 < 93 THEN 'General Surgery'
    ELSE 'Pulmonology'
  END as service_line_code,
  1 + cast(abs(hash(id + 1300)) % 5000 as int) as mbr_id,
  1 + cast(abs(hash(id + 1400)) % 15 as int) as rendering_prov_id,
  1 + cast(abs(hash(id + 1500)) % 10 as int) as svc_location_id,
  1 + cast(abs(hash(id + 1600)) % 10 as int) as insurance_id
FROM range(1, 50001) t(id);

-- ============================================================================
-- METRIC VIEW: claims_metrics
-- Governed metrics layer. Deployed up-front so during Session 0 Beat 5 the
-- instructor simply adds it to the Genie Space (no live CREATE needed).
--
-- Encodes business logic that Genie (and dashboards, alerts) can call via
-- MEASURE(). Especially valuable for Net Denial Rate, which requires nested
-- filters across initial_decision + appeal_decision that are easy to get
-- wrong when built from scratch each time.
-- ============================================================================

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

-- ============================================================================
-- VERIFICATION (optional — run to confirm the intentional skew landed)
-- ============================================================================

-- Naive unfiltered denial rate (should be ~18-19%, "this is way off"):
-- SELECT
--   round(count(case when initial_decision = 'DENIED' then 1 end) * 100.0 / count(*), 1) as naive_denial_rate_pct,
--   count(*) as total_claims
-- FROM genie_training.claims_analytics.claims;
--
-- Governed denial rate (should be ~8.2%, "this is right"):
-- SELECT
--   round(count(case when initial_decision = 'DENIED' then 1 end) * 100.0 / count(*), 1) as governed_denial_rate_pct,
--   count(*) as total_claims
-- FROM genie_training.claims_analytics.claims
-- WHERE voided_flag = 'N' AND test_flag = 'N'
--   AND claim_type IN ('Professional', 'Facility');
