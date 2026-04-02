# E-Commerce Analytics Pipeline
### Snowflake · dbt · Python · SQL | November 2019 | 67.5M Events

---

## Project Overview

End-to-end ELT analytics project built on a real-world e-commerce dataset with 67.5 million events. The project covers the full data lifecycle — from raw data ingestion to a cleaned star schema, tested dbt models, and a business analytics notebook with actionable insights.

---

## Tools & Technologies

| Tool | Purpose |
|---|---|
| **Snowflake** | Cloud data warehouse — storage, compute, querying |
| **dbt Cloud CLI** | Data transformation, modelling, testing |
| **Snowflake Notebooks** | Analysis and visualisation |
| **Python** | Pandas, Matplotlib, Seaborn, Scipy |
| **SQL** | Data investigation, transformation, analysis |

---

## Dataset

- **Source:** Kaggle — eCommerce behavior data from a multi-category store
- **File:** November 2019 (9GB CSV)
- **Rows:** 67,501,979 events
- **Events:** Views, cart adds, purchases
- **Period:** November 1–30, 2019

---

## Architecture

```
RAW LAYER
└── ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
    └── 67.5M rows loaded via Snowflake Stage + COPY INTO

STAGING LAYER (via dbt)
└── ECOMMERCE_ANALYTICS.STAGING.STG_EVENTS_CLEAN
    └── Cleaned, enriched, tested

MART LAYER (via dbt — Star Schema)
├── ECOMMERCE_ANALYTICS.MART.FCT_EVENTS
├── ECOMMERCE_ANALYTICS.MART.DIM_PRODUCTS
└── ECOMMERCE_ANALYTICS.MART.DIM_DATES

ANALYSIS LAYER
└── Snowflake Notebook
    └── Python + SQL analysis + visualizations
```

---

## Project Structure

```
ecommerce-analytics-pipeline/
├── README.md
├── .gitignore
├── dbt_project.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── stg_events_clean.sql
│   │   └── schema.yml
│   └── mart/
│       ├── fct_events.sql
│       ├── dim_products.sql
│       ├── dim_dates.sql
│       └── schema.yml
└── notebooks/
    └── ecommerce_analysis.ipynb
```

---

## Data Quality & Investigation

Before building the pipeline, a thorough data quality investigation was conducted on the raw 67.5M rows:

| Column | Nulls | % | Action |
|---|---|---|---|
| CATEGORY_CODE | 21,898,171 | 32.4% | Replace with 'unknown' |
| BRAND | 9,218,235 | 13.6% | Fix via window function, fallback 'unknown' |
| USER_SESSION | 10 | 0.0% | Drop rows |
| All others | 0 | 0% | Keep |

### Null Brand Fix
Some products had NULL brands where the same product appeared with a real brand in other events. Fixed using a window function — borrowing the brand from another event for the same product, with 'unknown' as a fallback:

```sql
COALESCE(
    BRAND,
    FIRST_VALUE(BRAND) IGNORE NULLS OVER (
        PARTITION BY PRODUCT_ID
        ORDER BY EVENT_TIME
    ),
    'unknown'
) AS BRAND
```

### Conflicting Brand Fix
136 products had two different real brand names for the same PRODUCT_ID (e.g. `nerf` vs `hasbro`, `bugatti` vs `bugati`). Since it was impossible to determine which brand was correct, all rows for these products were removed entirely:

```sql
PRODUCT_ID NOT IN (
    SELECT PRODUCT_ID
    FROM EVENTS_RAW
    WHERE BRAND IS NOT NULL
    GROUP BY PRODUCT_ID
    HAVING COUNT(DISTINCT BRAND) > 1
)
```

**Total data loss: 0.04% (26,467 rows removed) — Final staging rows: 67,475,512**

---

## dbt Models

### Staging — stg_events_clean
- Null handling for brands and category codes
- Removes products with conflicting brand names
- Extracts category hierarchy (CATEGORY_L1, CATEGORY_LEAF)
- Engineers date/time features (EVENT_DATE, EVENT_HOUR, DAY_NAME, IS_WEEKEND etc.)
- Creates binary event flags (IS_VIEW, IS_CART, IS_PURCHASE)

### Mart — Star Schema
- **fct_events** — core fact table with all events, measures and foreign keys
- **dim_products** — product attributes including category hierarchy and brand
- **dim_dates** — date dimension with day, week, month, quarter attributes

### dbt Tests
Full test coverage across all models using dbt schema files:
- `not_null` on all columns
- `accepted_values` for categorical columns
- `relationships` — referential integrity between fact and dimension tables
- `dbt_utils.expression_is_true` — PRICE >= 0

---

## How to Run

```bash
# Install dependencies
dbt deps

# Run staging model
dbt run --select stg_events_clean

# Run mart models
dbt run --select mart

# Run all tests
dbt test
```

---

## Analysis & Key Findings

### 1. Data Overview

- 3.7M unique users generated **67.5M events** in November 2019
- Each user averaged **18 interactions** and **3.7 sessions** during the month
- Average order value of **$300** suggests a high ticket electronics store
- **916K purchases** generated **$275M** in total revenue

---

### 2. Funnel Analysis
*Analyzing the customer journey from product view to purchase.*

- Only **4.77%** of views convert to cart
- **30.27%** of cart adds convert to purchase
- Overall conversion rate is **1.44%**
- Main drop-off point is view to cart — biggest opportunity for improvement

---

### 3. Revenue Analysis
*Identifying which categories drive the most revenue.*

- **Electronics dominates** with $205M revenue (74.6% of total)
- Electronics and computers have the highest AOV (~$400+)
- Unknown category accounts for **$29M** — suggests data quality impact
- Apparel has the lowest AOV at **$83** suggesting budget purchases

---

### 4. Top Brands Analysis
*Identifying which brands drive the most revenue and purchases.*

- **Apple leads** revenue at $127M with the highest AOV of $768
- **Samsung** sells more units (200K) but at a lower AOV of $274
- Two clear strategies: premium (Apple, Acer) vs volume (Samsung, Xiaomi)
- Top 2 brands account for **66% of total revenue**

---

### 5. Time Analysis
*Understanding when customers are most active.*

- Peak purchasing hours are **8AM – 10AM**
- 9AM is the busiest hour with **71K purchases** and **$22M revenue**
- Sharp drop-off after 6PM suggesting working hours drive purchases
- Midnight to 3AM has minimal activity

---

### 6. Day of Week Analysis
*Understanding which days drive the most purchases.*

- **Sunday** is by far the busiest day with **253K purchases** ($77M revenue)
- Weekends account for significantly more purchases than weekdays
- Weekday purchases are very consistent at ~97–105K per day
- AOV is consistent across all days ($290–$312) — day does not affect spend amount

---

### 7. A/B Testing — Weekend vs Weekday Conversion Rate
*Testing whether weekend conversion rates are statistically different from weekday.*

- Weekend conversion rate **(1.80%)** is **45% higher** than weekday (1.24%)
- P-value is essentially 0 → difference is **statistically significant**
- With 63M data points this is a genuine behavioral difference
- Business recommendation: increase marketing spend on weekends

---

### 8. Product Performance
*Identifying best performing products.*

- Top converting products achieve **10–18% conversion rate** vs 1.44% average
- Anomaly detected: Product 12712903 shows 22 purchases with 0 cart adds → possible direct purchase flow bypassing cart
- High conversion rate and high revenue are different optimization targets

---

### 9. User Behavior
*Understanding repeat buyers vs one time buyers.*

- **88% of users never made a purchase** → massive conversion opportunity
- Only **12% of users** ever purchased
- Of buyers, **14% became repeat purchasers**
- Recommendation: focus on converting browsers to first time buyers

---

## Business Recommendations

### 1. Fix the Funnel
View to cart rate is only 4.77% → add quick add to cart, social proof, and urgency triggers ("Only 3 left!")

### 2. Leverage Weekend & Morning Patterns
- Weekends convert 45% better → increase ad spend and launch products on weekends
- Peak hours 8–10AM → schedule campaigns and notifications for the morning window

### 3. Convert the 88% Browsers
- Retargeting campaigns, exit intent popups, first purchase discounts
- Personalized recommendations based on viewed products

### 4. Retain Existing Buyers
- Only 1.68% are repeat buyers → loyalty program and post purchase emails
- Existing buyers are cheaper to retain than acquiring new ones

### 5. Double Down on Electronics & Apple
- Electronics drives 74.6% of revenue → expand catalog
- Apple alone generates $127M → prioritize stock and cross-sell accessories

### 6. Recover Abandoned Carts
- 70% of cart adds never purchase → abandoned cart email reminders
- Even 5% recovery = significant revenue uplift

### 7. Fix Unknown Category
- $29M revenue unattributed → work with data team to categorize properly
- Better data at source = better insights

---

*Dataset: [eCommerce behavior data from multi-category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) — Kaggle*
