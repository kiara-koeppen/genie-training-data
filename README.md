# Genie Space Training Data

Synthetic healthcare claims data for a Genie Space training workshop. Supports a 3-session Data Analyst training track that teaches discovery, configuration, and testing of Databricks Genie Spaces.

## What's Included

| Table | Rows | Description |
|---|---|---|
| `facilities` | 10 | Hospital and clinic names with locations |
| `payers` | 10 | Insurance payer names (commercial, Medicare, Medicaid, self-pay) |
| `providers` | 15 | Provider names with medical specialties |
| `patients` | 5,000 | Synthetic patients with age groups, states, gender |
| `claims` | 50,000 | 18 months of claims data with adjudication, denial, and payment fields |
| `claims_summary` | (view) | Pre-joined view combining all tables |

## Key Metrics

The data is calibrated to produce realistic healthcare claims metrics:

- **Denial rate:** ~8.2% (Professional + Facility claims, excluding voided/test)
- **First-pass rate:** ~94.2%
- **Date range:** 18 months from January 2025
- **Claim types:** Professional (~45%), Facility (~45%), Pharmacy (~10%)
- **Payers:** Mix of commercial, Medicare, Medicaid, and self-pay

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

Then update the catalog/schema references in `setup.sql` to match (find and replace `ih_genie_training` with your catalog name).

### Step 2: Run the setup script

Open `setup.sql` in a Databricks SQL editor and run each statement sequentially. The script:

1. Creates 5 tables (facilities, payers, providers, patients, claims)
2. Inserts all data
3. Sets primary keys and foreign keys
4. Creates the pre-joined `claims_summary` view

**Note:** The SQL statements must be run one at a time (Databricks SQL does not support multiple statements separated by semicolons in a single execution). Either:
- Copy and paste each statement individually
- Use a notebook and put each statement in its own cell

### Step 3: Verify

Run the verification query at the bottom of `setup.sql` to confirm:
- Denial rate is ~8.2%
- First-pass rate is ~94.2%
- Total claims count is ~44,500 (Professional + Facility, excluding voided/test)

## Training Usage

This data supports a 3-session Genie Space training workshop:

- **Session 1 (90 min):** Discovery Mindset & Elicitation - uses two Genie Spaces (one under-configured, one well-configured) to demonstrate the impact of discovery
- **Session 2 (2 hr):** The Discovery Framework End-to-End - the training scenario is built around this data; participants reference the UC table schemas during data mapping exercises
- **Session 3 (90 min):** From Discovery to Configuration - the facilitator builds a Genie Space live from these tables while participants watch and then test together

## Genie Space Setup

After loading the data, create two Genie Spaces:

### 1. "Wrong" Genie Space (under-configured)
- Add all 5 tables
- No instructions, no entity matching, no SQL expressions
- Demonstrates what happens when you skip discovery

### 2. "Well Configured" Genie Space
- Add all 5 tables
- Configure entity matching for facility abbreviations, payer names, service lines, and domain terms
- Add SQL expressions for key metrics (Denial Rate, First Pass Rate, Average Turnaround Time)
- Add text instructions (scope boundaries, disambiguation, terminology, metric exclusions)
- Add joins between tables
- Add 10 sample questions

## Data Details

### Tables and Relationships

```
claims (fact table)
  -> patients     (via patient_id)
  -> providers    (via provider_id)
  -> facilities   (via facility_id)
  -> payers       (via payer_id)
```

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
| `billed_amount` | Amount billed by provider |
| `first_pass_rate` | Y = clean claim (passed all edits), N = required rework |
| `voided_flag` | Y = voided, N = active |
| `test_flag` | Y = test claim, N = real |
| `service_line_code` | Cardiovascular Services, Orthopedics, Primary Care, etc. |

## License

This is synthetic data generated for training purposes. No real patient, provider, or claims data is included.
