{% docs __overview__ %}
# Evidentia gold layer

This dbt project is the **ELT** step of the Evidentia lakehouse. The data has already been
**loaded** into Databricks: the PySpark job writes the silver Delta tables. dbt then
**transforms** it in place with SQL, inside the warehouse.

**Lineage:** `silver.papers` / `silver.claims` → `stg_*` views → `fct_claims`, `dim_papers` →
the three `gold_*` marts the Power BI dashboard reads.

- `gold_claims_by_intervention_outcome`: how many claims back each intervention → outcome pair, by result.
- `gold_weighted_disagreement`: how much the evidence disagrees, with RCTs counting more than case reports.
- `gold_confidence_distribution`: a histogram of the extractor's confidence by study design.

The data in the demo is synthetic (see `lakehouse/seed/`).
{% enddocs %}
