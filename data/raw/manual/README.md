This directory is for approved manual source drops (official artifacts only).

- Only place files here when automated fetch is blocked, restricted, or not approved.
- Use exact filenames by source id, for example:
  - `data_gov_in_nhai_projects_api.csv`
  - `data_gov_in_nhai_project_finance_api.csv`
  - `morth_annual_report_pdf.csv`
  - `ncrb_road_accidents_state_year.csv`
  - `morh_contractor_disclosures.csv`
  - `morh_arbitration_claims.csv`
  - `morh_procurement_awards.csv`
  - `ncrb_toll_fastag_claims.csv`
  - `parliament_qa_highway_queries.csv`
  - `quality_maintenance_indicators.csv`
  - `rbi_mospi_macro_indicators.csv`
  - `viirs_nightlights_proxy.csv`

Each manual file is copied into source manifests with checksum and treated as the input for connector parsing.
