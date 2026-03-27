# Genie Space Training Data - Intermountain Health

Synthetic claims data for the Genie Space training workshop. This data supports all three sessions of the Data Analyst training track.

## What's Included

| Table | Rows | Description |
|---|---|---|
| `facilities` | 10 | IH facility names (IMC, LDS, PCMC, McKay-Dee, etc.) |
| `payers` | 10 | Payer names (BCBS of Utah, UnitedHealthcare, Aetna, etc.) |
| `providers` | 15 | Provider names with specialties |
| `patients` | 5,000 | Synthetic patients with age groups, states, gender |
| `claims` | 50,000 | 18 months of claims data with all training-relevant fields |
| `claims_summary` | (view) | Pre-joined view combining all tables |

## Key Metrics (calibrated to match training examples)

- **Denial rate:** ~8.2% (Professional + Facility claims, excluding voided/test)
- **First-pass rate:** ~94.2%
- **Date range:** 18 months from January 2025
- **IH facilities:** Intermountain Medical Center (IMC), LDS Hospital, Primary Childrens Medical Center (PCMC), McKay-Dee, Dixie Regional, Utah Valley, Logan Regional, Cassia Regional, St. James, Cedar City
- **Payers:** BCBS of Utah, UnitedHealthcare, Aetna, Cigna, Regence, Medicare, Medicaid, SelectHealth, DMBA, Self-Pay

## Setup

### Prerequisites

1. A Databricks workspace with Unity Catalog enabled
2. Permission to create catalogs and schemas (or ask an admin)
3. A SQL warehouse or cluster

### Step 1: Create the catalog and schema

Run these in a SQL editor (requires admin privileges):

```sql
CREATE CATALOG IF NOT EXISTS ih_genie_training;
CREATE SCHEMA IF NOT EXISTS ih_genie_training.claims_analytics;
```

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

### Session 1: Discovery Mindset & Elicitation
- The "Wrong Genie Space" demo uses these tables with minimal configuration
- The "Well Configured Genie Space" demo uses these tables with full entity matching, SQL expressions, and instructions

### Session 2: The Discovery Framework End-to-End
- The Dana scenario (Claims Ops Manager) is built around this data
- Workbook 3 (Data Mapping) references these UC table schemas
- Workbook 4 (Prototype Review) tests against a Genie Space connected to this data

### Session 3: From Discovery to Configuration
- The facilitator builds a Genie Space live using these tables
- Live testing runs against this data

## Genie Space Configuration

After loading the data, you'll need two Genie Spaces:

### 1. "Training Demo - Wrong Genie Space"
- Add all 5 tables
- No instructions, no entity matching, no SQL expressions
- This demonstrates what happens when you skip discovery

### 2. "Training Demo - Well Configured Genie Space"
- Add all 5 tables
- Configure entity matching (IMC, LDS, PCMC, Blue Cross, United, cardiology, member/patient/enrollee)
- Add SQL expressions (Denial Rate, First Pass Rate, Average Turnaround Time)
- Add text instructions (scope, disambiguation, terminology, exclusions)
- Add joins (claims to patients, providers, facilities, payers)
- Add 10 sample questions

See the instructor guide for Session 3 for the full configuration checklist.
