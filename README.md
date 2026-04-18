# Genie Space Training Data

Synthetic healthcare claims data for a Genie Space training workshop. Supports a 4-session Data Analyst training track that teaches discovery, configuration, and testing of Databricks Genie Spaces.

## What's Included

| Table | Rows | Description |
|---|---|---|
| `facilities` | 10 | Hospital and clinic names with locations |
| `payers` | 10 | Insurance payer names (commercial, Medicare, Medicaid, self-pay) |
| `providers` | 15 | Provider names with medical specialties |
| `patients` | 5,000 | Synthetic patients with age groups, states, gender |
| `members` | 5,000 | Plan enrollment records (plan type, plan tier, effective dates) |
| `claims` | 50,000 | 18 months of claims data with adjudication, denial, and payment fields |
| `claims_metrics` | (metric view) | Governed metrics layer with standard measure definitions |

## Key Metrics

The data is calibrated to produce realistic healthcare claims metrics:

- **Naive denial rate:** ~18.5% (all claims, Pharmacy inflated at ~60% per its pattern)
- **Governed denial rate:** ~8.2% (Professional + Facility claims, excluding voided/test)
- **Net denial rate:** ~7.2% (denied and stayed denied — appeal not filed or appeal upheld)
- **First-pass rate:** ~94.2%
- **Appeal overturn rate:** ~40%
- **Average turnaround:** ~12 days
- **Date range:** 18 months from January 2025
- **Claim types:** Professional (~45%), Facility (~45%), Pharmacy (~10%)
- **Billed amount distribution:** long-tailed — 70% under $5K, 25% $5K-$15K, 5% above $15K

## Setup

### Prerequisites

1. A Databricks workspace with Unity Catalog enabled
2. Permission to create catalogs and schemas (or ask an admin)
3. A SQL warehouse or cluster

### Step 1: Create the catalog and schema

Run these in a SQL editor (requires admin privileges). Replace the catalog/schema names with your own if needed:

```sql
CREATE CATALOG IF NOT EXISTS genie_training;
CREATE SCHEMA IF NOT EXISTS genie_training.claims_analytics;
```

If you use a different catalog name, find and replace `genie_training` in `setup.sql` to match.

### Step 2: Run the setup script

Open `setup.sql` in a Databricks SQL editor and run each statement sequentially. The script:

1. Creates 6 tables (facilities, payers, providers, patients, members, claims)
2. Inserts all data
3. Creates the `claims_metrics` metric view with governed measure definitions

Tables are left bare (no PK/FK constraints, no column comments) — the training demonstrates how to layer configuration at the Genie Space level rather than in Unity Catalog metadata.

**Note:** The SQL statements must be run one at a time (Databricks SQL does not support multiple statements separated by semicolons in a single execution). Either:
- Copy and paste each statement individually
- Use a notebook and put each statement in its own cell

### Step 3: Verify

Run the verification queries at the bottom of `setup.sql` to confirm:
- Naive denial rate is ~18.5%
- Governed denial rate is ~8.2%
- First-pass rate is ~94.2%

## Training Usage

This data supports a 4-session Genie Space training workshop:

- **Session 0 (60 min):** What is Genie — a single Space evolves through 5 progressive configuration beats (text instructions → joins → column descriptions → SQL functions → metric view). See the Session 0 Instructor Guide for the live script.
- **Session 1 (90 min):** Discovery Mindset & Elicitation — how to interview stakeholders before touching configuration.
- **Session 2 (2 hr):** The Discovery Framework End-to-End — participants run discovery against this dataset.
- **Session 3 (90 min):** From Discovery to Configuration — facilitator builds a Space live while participants watch and test.

## Genie Space Setup

Create a single Genie Space with all 6 tables added. No initial configuration. Through Session 0, the instructor layers configuration in front of participants in 5 beats:

1. **Text instructions** — business terminology, scope boundaries, exclusions (e.g., exclude Pharmacy from denial rate)
2. **Joins** — explicit join definitions for non-obvious relationships (claims ↔ members)
3. **Column descriptions** — added in the Space Data tab (not Unity Catalog), scoped locally
4. **SQL queries & functions** — saved CASE statements for reusable bucketing logic (e.g., high-dollar vs low-dollar)
5. **Metric views** — attach the governed `claims_metrics` metric view

The `claims_metrics` metric view is pre-deployed by `setup.sql`, so Beat 5 is just "add the existing metric view to the Space." All other fixes are applied in the Space itself — exact paste text and expected before/after values are in the Session 0 Instructor Guide.

## Metric View: `claims_metrics`

The `claims_metrics` metric view is the governed metrics layer for claims analytics. It encodes the business logic (what "denial rate" means, what to exclude, how to calculate it) so that every consumer — Genie, dashboards, alerts — gets the same answer.

### What it includes

**Global filter:** Excludes voided and test claims automatically, so no downstream consumer has to remember to add those filters.

**Joins:** Connects the claims fact table to all four dimension tables (patients, providers, facilities, payers) using the renamed FK columns (`mbr_id`, `rendering_prov_id`, `svc_location_id`, `insurance_id`).

**11 dimensions:** Claim Type, Service Line, Initial Decision, Appeal Decision, Receipt Month, Payer Name, Facility Name, Facility State, Provider Specialty, Patient Age Group, Patient State.

**8 measures:**

| Measure | Description | Expected Value |
|---|---|---|
| Total Claims | Count of all non-voided, non-test claims | ~49,500 |
| Denial Rate | % denied (Professional + Facility only) | ~8.2% |
| Net Denial Rate | % denied and stayed denied (no appeal or appeal upheld) | ~7.2% |
| First Pass Rate | % passing all edits on first submission | ~94.2% |
| Appeal Overturn Rate | % of filed appeals overturned | ~40% |
| Average Turnaround Days | Avg days from receipt to adjudication | ~12 |
| Total Paid Amount | Sum of all payments | varies |
| Total Billed Amount | Sum of all billed charges | varies |

### Why a metric view instead of a regular view

A regular view just pre-joins tables. A metric view encodes the business logic so that every consumer gets the same answer. This is a core concept in the training: define once, govern centrally, use everywhere.

### Querying

Metric views use the `MEASURE()` function:

```sql
SELECT
  `Payer Name`,
  MEASURE(`Denial Rate`) AS denial_rate,
  MEASURE(`Net Denial Rate`) AS net_denial_rate,
  MEASURE(`First Pass Rate`) AS first_pass_rate
FROM genie_training.claims_analytics.claims_metrics
GROUP BY ALL
ORDER BY denial_rate DESC
```

## Data Details

### Tables and Relationships

```
claims (fact table)
  -> patients     (via mbr_id = patient_id)
  -> providers    (via rendering_prov_id = provider_id)
  -> facilities   (via svc_location_id = facility_id)
  -> payers       (via insurance_id = payer_id)

members
  -> patients     (via subscriber_num = patient_id)
```

The `members` table uses `subscriber_num` (no name overlap with `patients.patient_id`) plus `MEM-`-prefixed `member_id` values. The claims table also carries a decoy `member_num` column (MEM-prefixed, out-of-range values) that tempts Genie toward the wrong join — this is intentional, to force explicit join declarations during Beat 2.

### Key Columns (claims table)

| Column | Description |
|---|---|
| `claim_id` | Unique claim identifier (CLM-0000001 format) |
| `claim_type` | Professional, Facility, or Pharmacy |
| `receipt_date` | Date claim was received |
| `adjudication_date` | Date claim was adjudicated (NULL if pending) |
| `initial_decision` | APPROVED, DENIED, PENDING, or PARTIAL |
| `appeal_decision` | OVERTURNED, UPHELD, or NULL |
| `paid_amount` | Amount paid (0 for denied, NULL for pending) |
| `billed_amount` | Amount billed by provider (long-tailed distribution) |
| `aa_ind` | Auto-adjudicated indicator. Y = claim passed all edits on first submission (first-pass), N = required rework. *Beat 3 trap — cryptic column name.* |
| `voided_flag` | Y = voided, N = active |
| `test_flag` | Y = test claim, N = real |
| `service_line_code` | Cardiovascular Services, Orthopedics, Primary Care, etc. |
| `mbr_id` | FK to patients.patient_id |
| `rendering_prov_id` | FK to providers.provider_id |
| `svc_location_id` | FK to facilities.facility_id |
| `insurance_id` | FK to payers.payer_id |
| `member_num` | *Beat 2 decoy.* Looks like `members.member_id` but values live in an out-of-range space so the "obvious" join returns 0 rows. |

## License

This is synthetic data generated for training purposes. No real patient, provider, or claims data is included.
