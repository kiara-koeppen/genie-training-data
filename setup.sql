-- Genie Space Training Data Setup Script
-- Healthcare Claims Analytics
--
-- This script creates synthetic claims data for the Genie Space training workshop.
-- The data is calibrated to match the training examples:
--   - Denial rate: ~8.2%
--   - First-pass rate: ~94.2%
--   - 50,000 claims across 18 months
--   - Healthcare facility names, payer names, service lines
--
-- Prerequisites:
--   1. Create the catalog and schema before running this script:
--      CREATE CATALOG IF NOT EXISTS genie_training;
--      CREATE SCHEMA IF NOT EXISTS genie_training.claims_analytics;
--   2. Update all references below from 'ih_genie_training' to your catalog name
--   3. Run this script on a SQL warehouse or cluster with access to the catalog
--
-- Usage:
--   Run each statement one at a time in a Databricks SQL editor,
--   or put each statement in its own notebook cell

-- ============================================================================
-- FACILITIES
-- ============================================================================

CREATE OR REPLACE TABLE ih_genie_training.claims_analytics.facilities (
  facility_id INT NOT NULL,
  facility_name STRING COMMENT 'Full name of the facility where service was rendered. Common abbreviations: IMC = Intermountain Medical Center, LDS = LDS Hospital, PCMC = Primary Childrens Medical Center',
  facility_type STRING COMMENT 'Type of facility: Hospital, Clinic, Urgent Care, Specialty Center',
  city STRING,
  state_code STRING COMMENT 'Two-letter state code. Service area includes UT, ID, NV, CO, MT, WY, KS, NE'
);

INSERT INTO ih_genie_training.claims_analytics.facilities VALUES
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
-- ============================================================================

CREATE OR REPLACE TABLE ih_genie_training.claims_analytics.payers (
  payer_id INT NOT NULL,
  payer_name STRING COMMENT 'Full payer name. Common abbreviations: Blue Cross = BCBS of Utah, United = UnitedHealthcare',
  payer_type STRING COMMENT 'Payer category: Commercial, Medicare, Medicaid, Self-Pay'
);

INSERT INTO ih_genie_training.claims_analytics.payers VALUES
(1, 'BCBS of Utah', 'Commercial'),
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

CREATE OR REPLACE TABLE ih_genie_training.claims_analytics.providers (
  provider_id INT NOT NULL,
  provider_name STRING COMMENT 'Full name of the rendering provider',
  provider_specialty STRING COMMENT 'Medical specialty. Common abbreviations: cardiology = Cardiovascular Services',
  provider_npi STRING COMMENT 'National Provider Identifier (10-digit)'
);

INSERT INTO ih_genie_training.claims_analytics.providers VALUES
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

CREATE OR REPLACE TABLE ih_genie_training.claims_analytics.patients (
  patient_id INT NOT NULL,
  patient_age_group STRING COMMENT 'Age band: 0-17, 18-34, 35-49, 50-64, 65+',
  patient_state STRING COMMENT 'State of patient residence',
  patient_gender STRING COMMENT 'M or F'
);

INSERT INTO ih_genie_training.claims_analytics.patients
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
-- CLAIMS (50,000 synthetic claims)
-- Calibrated to produce:
--   Denial rate: ~8.2% (for Professional + Facility, excluding voided/test)
--   First-pass rate: ~94.2%
--   Dates: 18 months of data from Jan 2025
-- ============================================================================

CREATE OR REPLACE TABLE ih_genie_training.claims_analytics.claims (
  claim_id STRING COMMENT 'Unique identifier for each claim submission',
  claim_type STRING COMMENT 'Type of claim: Professional, Facility, or Pharmacy',
  receipt_date DATE COMMENT 'Date the claim was received. Used as the basis for turnaround time calculations',
  adjudication_date DATE COMMENT 'Date the claim was adjudicated. NULL for claims still pending',
  initial_decision STRING COMMENT 'The adjudication outcome at first review. Values: APPROVED, DENIED, PENDING, PARTIAL. Does not reflect appeal outcomes',
  appeal_decision STRING COMMENT 'Outcome after appeal if applicable. Values: OVERTURNED, UPHELD, NULL (no appeal filed)',
  paid_amount DECIMAL(12,2) COMMENT 'Total amount paid on the claim after all adjustments. Zero for denied claims. NULL for claims still in adjudication',
  billed_amount DECIMAL(12,2) COMMENT 'Total amount billed by the provider',
  first_pass_rate STRING COMMENT 'Whether the claim passed all edits on first submission. Y = clean claim, N = required rework',
  voided_flag STRING COMMENT 'Y if claim was voided, N if active. Voided claims should be excluded from most metrics',
  test_flag STRING COMMENT 'Y if this is a test claim, N if real. Test claims should be excluded from all metrics',
  service_line_code STRING COMMENT 'Service line category for the encounter',
  patient_id INT,
  provider_id INT,
  facility_id INT,
  payer_id INT
);

INSERT INTO ih_genie_training.claims_analytics.claims
SELECT
  concat('CLM-', lpad(cast(id as string), 7, '0')) as claim_id,
  CASE
    WHEN abs(hash(id)) % 100 < 45 THEN 'Professional'
    WHEN abs(hash(id)) % 100 < 90 THEN 'Facility'
    ELSE 'Pharmacy'
  END as claim_type,
  date_add('2025-01-01', cast(abs(hash(id + 100)) % 450 as int)) as receipt_date,
  CASE
    WHEN abs(hash(id + 200)) % 100 < 2 THEN NULL
    ELSE date_add(date_add('2025-01-01', cast(abs(hash(id + 100)) % 450 as int)), 3 + cast(abs(hash(id + 300)) % 19 as int))
  END as adjudication_date,
  CASE
    WHEN abs(hash(id + 200)) % 100 < 2 THEN 'PENDING'
    WHEN abs(hash(id + 400)) % 1000 < 82 THEN 'DENIED'
    WHEN abs(hash(id + 400)) % 1000 < 112 THEN 'PARTIAL'
    ELSE 'APPROVED'
  END as initial_decision,
  CASE
    WHEN abs(hash(id + 400)) % 1000 < 82 AND abs(hash(id + 500)) % 100 < 30 THEN
      CASE WHEN abs(hash(id + 600)) % 100 < 40 THEN 'OVERTURNED' ELSE 'UPHELD' END
    ELSE NULL
  END as appeal_decision,
  CASE
    WHEN abs(hash(id + 400)) % 1000 < 82 THEN 0.00
    WHEN abs(hash(id + 200)) % 100 < 2 THEN NULL
    ELSE round(200 + cast(abs(hash(id + 700)) % 4800 as decimal(12,2)), 2)
  END as paid_amount,
  round(300 + cast(abs(hash(id + 800)) % 7700 as decimal(12,2)), 2) as billed_amount,
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
  1 + cast(abs(hash(id + 1300)) % 5000 as int) as patient_id,
  1 + cast(abs(hash(id + 1400)) % 15 as int) as provider_id,
  1 + cast(abs(hash(id + 1500)) % 10 as int) as facility_id,
  1 + cast(abs(hash(id + 1600)) % 10 as int) as payer_id
FROM range(1, 50001) t(id);

-- ============================================================================
-- PRIMARY KEYS
-- ============================================================================

ALTER TABLE ih_genie_training.claims_analytics.facilities ADD CONSTRAINT pk_facilities PRIMARY KEY (facility_id);
ALTER TABLE ih_genie_training.claims_analytics.payers ADD CONSTRAINT pk_payers PRIMARY KEY (payer_id);
ALTER TABLE ih_genie_training.claims_analytics.providers ADD CONSTRAINT pk_providers PRIMARY KEY (provider_id);
ALTER TABLE ih_genie_training.claims_analytics.patients ADD CONSTRAINT pk_patients PRIMARY KEY (patient_id);

-- ============================================================================
-- FOREIGN KEYS
-- ============================================================================

ALTER TABLE ih_genie_training.claims_analytics.claims ADD CONSTRAINT fk_claims_patients FOREIGN KEY (patient_id) REFERENCES ih_genie_training.claims_analytics.patients(patient_id);
ALTER TABLE ih_genie_training.claims_analytics.claims ADD CONSTRAINT fk_claims_providers FOREIGN KEY (provider_id) REFERENCES ih_genie_training.claims_analytics.providers(provider_id);
ALTER TABLE ih_genie_training.claims_analytics.claims ADD CONSTRAINT fk_claims_facilities FOREIGN KEY (facility_id) REFERENCES ih_genie_training.claims_analytics.facilities(facility_id);
ALTER TABLE ih_genie_training.claims_analytics.claims ADD CONSTRAINT fk_claims_payers FOREIGN KEY (payer_id) REFERENCES ih_genie_training.claims_analytics.payers(payer_id);

-- ============================================================================
-- TABLE-LEVEL COMMENTS
-- ============================================================================

COMMENT ON TABLE ih_genie_training.claims_analytics.facilities IS 'Healthcare facilities where services are rendered. Includes hospitals, clinics, and specialty centers across the service area.';
COMMENT ON TABLE ih_genie_training.claims_analytics.payers IS 'Insurance payers including commercial carriers, Medicare, Medicaid, and self-pay.';
COMMENT ON TABLE ih_genie_training.claims_analytics.providers IS 'Rendering providers with name, specialty, and NPI.';
COMMENT ON TABLE ih_genie_training.claims_analytics.patients IS 'Synthetic patient records with demographics. No real patient data.';
COMMENT ON TABLE ih_genie_training.claims_analytics.claims IS 'Claims fact table with adjudication outcomes, payment amounts, and quality flags. Joins to patients, providers, facilities, and payers via foreign keys.';

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Run this to verify the data matches training examples:
-- SELECT
--   round(count(case when initial_decision = 'DENIED' then 1 end) * 100.0 / count(*), 1) as denial_rate_pct,
--   round(count(case when first_pass_rate = 'Y' then 1 end) * 100.0 / count(*), 1) as first_pass_rate_pct,
--   count(*) as total_claims
-- FROM ih_genie_training.claims_analytics.claims
-- WHERE voided_flag = 'N' AND test_flag = 'N'
--   AND claim_type IN ('Professional', 'Facility');
-- Expected: denial_rate ~8.2%, first_pass_rate ~94.2%, total_claims ~44,500
