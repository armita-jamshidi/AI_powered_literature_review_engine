# Power BI dashboard on the gold layer

Power BI Desktop connects to the Databricks SQL warehouse, imports the gold tables that dbt built in `workspace.evidentia_gold`, and shows them on one report page. The saved report is `evidentia_gold.pbix` in this folder, with a screenshot in `dashboard.png`.

Power BI Desktop is free (Windows only). Install it from the Microsoft Store.

## 1. Connect to Databricks

1. Open Power BI Desktop and choose **Blank report**.
2. Click **Get data → More…**, search for **Databricks**, pick **Azure Databricks**, and click **Connect**. Despite its name, this connector also works for Databricks on AWS, which is what Free Edition runs on.
3. Fill in:
   - **Server Hostname:** your `DATABRICKS_HOST` *without* `https://`, e.g. `dbc-xxxx-xxxx.cloud.databricks.com`
   - **HTTP Path:** your `DATABRICKS_HTTP_PATH`, e.g. `/sql/1.0/warehouses/abc123`
   - **Data Connectivity mode:** **Import**
4. When it asks how to sign in, choose **Personal Access Token** and paste your Databricks token. Power BI keeps the token in its own credential store on your PC, not inside the `.pbix` file.
5. In the Navigator, open **workspace → evidentia_gold** and tick:
   - `fct_claims`
   - `dim_papers`
   - `gold_weighted_disagreement`
   - `gold_claims_by_intervention_outcome`
6. Click **Load**.

**Import vs DirectQuery:** Import copies the data into the report, so visuals are fast and the warehouse can sleep; you click **Refresh** to pick up new data. DirectQuery would send a SQL query to the warehouse on every click. That suits big or fast-changing data, but it keeps the warehouse running.

## 2. Relate the tables

Open **Model view** (the third icon on the left). Delete any relationships Power BI guessed, then drag these:

| From (many side) | To (one side) |
|---|---|
| `fct_claims[paper_id]` | `dim_papers[paper_id]` |
| `fct_claims[claim_pair]` | `gold_weighted_disagreement[claim_pair]` |
| `gold_claims_by_intervention_outcome[claim_pair]` | `gold_weighted_disagreement[claim_pair]` (one-to-one) |

This is a small **star schema**: `fct_claims` is the fact table in the middle, and the other tables describe papers and intervention × outcome pairs. With these links, a slicer on intervention filters every visual on the page.

## 3. Build the page

Go back to **Report view** and add these visuals:

1. **Slicer:** field `gold_weighted_disagreement[intervention]`.
2. **Three cards:** count of `fct_claims[claim_id]` (title "Claims"), distinct count of `fct_claims[paper_id]` ("Papers"), and count of `gold_weighted_disagreement[claim_pair]` ("Intervention × outcome pairs").
3. **Clustered bar chart, "Where the evidence disagrees":**
   - Y-axis: `gold_weighted_disagreement[claim_pair]`
   - X-axis: `weighted_disagreement_score` and `unweighted_disagreement_score`, both set to *Don't summarize* or *Sum*
   - Sort by `weighted_disagreement_score`, descending
4. **100% stacked bar chart, "What the studies found":**
   - Y-axis: `gold_claims_by_intervention_outcome[claim_pair]`
   - X-axis: `n_positive`, `n_negative`, `n_null`, `n_mixed`
5. **Stacked column chart, "Extractor confidence by study design":**
   - X-axis: `fct_claims[confidence_bucket]`
   - Y-axis: count of `fct_claims[claim_id]`
   - Legend: `fct_claims[study_type]`

Click a bar or pick an intervention in the slicer, and the other visuals cross-filter.

## 4. Save it to the repo

1. **File → Save as**, saved as `lakehouse\bi\evidentia_gold.pbix` in the repo.
2. Take a screenshot of the page and save it as `lakehouse\bi\dashboard.png`.
3. Commit both. The data is synthetic and the file contains no credentials.

To refresh after a new pipeline run (export → silver → `dbt build`), click **Home → Refresh**.

## Teardown

Nothing to tear down: Power BI Desktop is a local app. Deleting the Databricks token (see `lakehouse/README.md`) stops the report from refreshing.
