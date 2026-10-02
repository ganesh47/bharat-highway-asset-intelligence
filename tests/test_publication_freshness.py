import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipelines.common import sha256_for_file
from pipelines.connectors.primary_disclosures import SnapshotBuilder, PrimaryDisclosuresConnector, validate_facts, research_cutoff
from pipelines.ingest import run_ingestion
from pipelines.metric_coverage import metric_coverage, quarter_context


class PublicationHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sid = "union_budget_morth_demand86"
        self.csv = self.root/"manual"/f"{self.sid}.csv"
        self.evidence = self.root/"manual/evidence"/f"{self.sid}.json"

    def tearDown(self):
        self.temp.cleanup()

    def builder(self, document, year, value, published="", **extra):
        path = self.root/"primary_disclosures"/self.sid/"document.pdf"
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(b"%PDF-"+document.encode())
        builder = SnapshotBuilder(self.root,"2026-10-02")
        builder.pin(self.sid,path,"https://example.gov.in/"+document+".pdf")
        builder.fact(self.sid,"reported_spending",value,"INR crore","p1 table1",start=f"{year-1}-04-01",end=f"{year}-03-31",published=published,**extra)
        return builder

    def test_new_publication_appends_period_and_idempotent_rebuild_keeps_history(self):
        self.builder("first",2024,10,published="2024-07-01").finish((self.sid,))
        first = json.loads(self.evidence.read_text())
        original_doc = self.root/first["documents"][0]["relative_path"]
        self.builder("second",2025,20,published="2025-07-01").finish((self.sid,))
        current = pd.read_csv(self.csv)
        self.assertEqual(set(current.period_end),{"2024-03-31","2025-03-31"})
        self.assertEqual(list(current.value),[10,20])
        self.assertEqual(sha256_for_file(original_doc),first["documents"][0]["sha256"])
        before_csv = self.csv.read_bytes()
        self.builder("second",2025,20,published="2025-07-01").finish((self.sid,))
        index = json.loads((self.root/"manual/history"/self.sid/"index.json").read_text())
        versions = len(index["versions"])
        self.builder("second",2025,20,published="2025-07-01").finish((self.sid,))
        self.assertEqual(self.csv.read_bytes(),before_csv)
        self.assertEqual(len(json.loads((self.root/"manual/history"/self.sid/"index.json").read_text())["versions"]),versions)
        for version in index["versions"]:
            self.assertEqual(sha256_for_file(self.root/"manual"/version["facts_path"]),version["csv_sha256"])

    def test_conflicting_undated_cell_preserves_published_csv_and_evidence(self):
        self.builder("first",2024,10).finish((self.sid,))
        before = (self.csv.read_bytes(),self.evidence.read_bytes())
        with self.assertRaisesRegex(ValueError,"canonical_conflict_requires_revision"):
            self.builder("second",2024,11).finish((self.sid,))
        self.assertEqual((self.csv.read_bytes(),self.evidence.read_bytes()),before)

    def test_later_published_revision_replaces_current_cell_but_preserves_prior(self):
        self.builder("first",2024,10,published="2024-07-01").finish((self.sid,))
        self.builder("revision",2024,11,published="2024-08-01",observation_status="revised").finish((self.sid,))
        self.assertEqual(list(pd.read_csv(self.csv).value),[11])
        index=json.loads((self.root/"manual/history"/self.sid/"index.json").read_text())
        self.assertEqual(len(index["revisions"]),1)
        values={float(pd.read_csv(self.root/"manual"/version["facts_path"]).iloc[0].value) for version in index["versions"]}
        self.assertEqual(values,{10,11})

    def test_missing_new_extract_does_not_erase_validated_publication(self):
        self.builder("first",2024,10).finish((self.sid,))
        before=(self.csv.read_bytes(),self.evidence.read_bytes())
        empty=SnapshotBuilder(self.root,"2026-10-02")
        empty.finish((self.sid,))
        self.assertEqual((self.csv.read_bytes(),self.evidence.read_bytes()),before)

    def test_run_cutoff_rejects_future_actual_without_freezing_future_runs(self):
        builder=self.builder("first",2026,10)
        frame=pd.DataFrame(builder.rows[self.sid])
        evidence={"documents":builder.documents[self.sid]}
        with self.assertRaisesRegex(ValueError,"cutoff|Future actual"):
            validate_facts(frame,self.sid,evidence,cutoff="2026-03-01")
        self.assertEqual(len(validate_facts(frame,self.sid,evidence,cutoff="2026-04-01")),1)
        with patch.dict("os.environ",{"BHAI_RESEARCH_CUTOFF":"2027-01-02"}):
            self.assertEqual(research_cutoff(),"2027-01-02")

    def test_first_publication_keeps_governed_local_facts_after_remote_failure(self):
        import yaml
        self.builder("first",2024,10,published="2024-07-01").finish((self.sid,))
        evidence=json.loads(self.evidence.read_text())
        inventory=self.root/"inventory.yaml"
        source={"source_id":self.sid,"allow_auto_fetch":True,"official_flag":True,"publisher_org":"Official",
                "publisher_type":"government","update_frequency":"annual","license_terms":"Public attribution"}
        inventory.write_text(yaml.safe_dump({"sources":[source]}))
        with patch("pipelines.connectors.primary_disclosures.download_document",side_effect=ValueError("Response is not a PDF")),patch.dict("os.environ",{"BHAI_PRIMARY_REMOTE_CHECK":"1"}):
            entry=run_ingestion(str(inventory),raw_root=self.root,processed_root=self.root/"processed",
                                manifest_root=self.root/"manifests",catalog_path=self.root/"manifests/catalog.json",cutoff="2026-10-02")[self.sid]
        self.assertTrue(entry["analytical_ready"])
        self.assertEqual(entry["refresh_outcome"],"retained_after_failure")
        self.assertEqual(entry["source_as_of_date"],"2024-03-31")
        self.assertEqual(entry["publication_date"],"2024-07-01")
        self.assertEqual(entry["last_successful_retrieval_at"],evidence["retrieved_at"])
        self.assertEqual(list(pd.read_parquet(self.root/"processed"/f"{self.sid}.parquet").value),[10])


class MetricFreshnessTests(unittest.TestCase):
    source={"source_id":"monthly", "next_expected_publication_at":"2026-10-26"}
    entry={"evidence_status":"verified", "analytical_ready":True}

    def monthly(self,months):
        rows=[]
        for month in months:
            rows.append({"entity_id":"network", "metric":"payments", "value":10,"source_document_sha256":"a"*64,
                         "unit":"INR crore","agency":"NPCI","road_class":"payment_network", "period_basis":"calendar_month",
                         "statement_basis":"reported_network","estimate_type":"actual","period_start":f"2026-{month:02}-01",
                         "period_end":f"2026-{month:02}-30" if month==9 else f"2026-{month:02}-31",
                         "data_as_of":f"2026-{month:02}-30" if month==9 else f"2026-{month:02}-31",
                         "published_at":"", "observation_status":"reported"})
        return pd.DataFrame(rows)

    def test_complete_quarter_requires_three_months_and_not_current_quarter_projection(self):
        partial=metric_coverage(self.monthly([7,8]),self.source,self.entry,"2026-10-02")[0]
        self.assertIsNone(partial["latest_complete_quarter_end"])
        self.assertEqual(partial["current_quarter_coverage"],"not_yet_reported")
        complete=metric_coverage(self.monthly([7,8,9]),self.source,self.entry,"2026-10-02")[0]
        self.assertEqual(complete["latest_complete_quarter_end"],"2026-09-30")
        self.assertEqual(complete["coverage_status"],"publication_pending")
        self.assertEqual(complete["next_expected_publication_at"],"2026-10-26")
        self.assertIsNone(complete["latest_publication_date"])
        self.assertIsNone(complete["publication_lag_days"])
        self.assertEqual(complete["calendar_quarter"],"2026-Q4")
        self.assertEqual(complete["fiscal_quarter"],"FY2026-27-Q3")

    def test_annual_state_footnotes_and_old_metrics_do_not_get_global_latest_cutoff(self):
        frame=self.monthly([7]).iloc[[0,0]].copy()
        frame["entity_id"]=["state_a","state_b"]
        frame["metric"]="sh_network_length_km"
        frame["period_basis"]="fiscal_year"
        frame["period_end"]=frame["data_as_of"]=["2018-03-31","2022-03-31"]
        frame["published_at"]="2026-08-01"
        record=metric_coverage(frame,{"source_id":"SH"},self.entry,"2026-10-02")[0]
        self.assertEqual(record["entity_cutoffs"],{"state_a":"2018-03-31","state_b":"2022-03-31"})
        self.assertEqual(record["earliest_entity_cutoff"],"2018-03-31")
        self.assertEqual(record["latest_observation_date"],"2022-03-31")
        self.assertEqual(record["current_quarter_coverage"],"not_applicable")

    def test_unverified_claimed_year_never_enters_metric_freshness(self):
        frame=self.monthly([7])
        record=metric_coverage(frame,self.source,{"evidence_status":"unverified","source_as_of_date":None},"2026-10-02")[0]
        self.assertEqual(record["coverage_status"],"not_calculation_ready")
        self.assertIsNone(record["latest_observation_date"])

    def test_provisional_is_explicit_and_does_not_mean_current_quarter_complete(self):
        frame=self.monthly([7]); frame["observation_status"]="provisional";frame["published_at"]="2026-08-04"
        record=metric_coverage(frame,{"source_id":"monthly"},self.entry,"2026-08-05")[0]
        self.assertEqual(record["coverage_status"],"official_provisional")
        self.assertEqual(record["current_quarter_coverage"],"partial_reported")
        self.assertEqual(record["publication_lag_days"],4)


if __name__=="__main__":
    unittest.main()
