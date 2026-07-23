import importlib.util
import logging
import pathlib
import sys
import unittest


logging.disable(logging.CRITICAL)
SCRIPT_PATH = pathlib.Path(__file__).parents[3] / "scripts" / "backfill_sonar_webapp_client.py"
SPEC = importlib.util.spec_from_file_location("backfill_sonar_webapp_client", SCRIPT_PATH)
backfill = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["backfill_sonar_webapp_client"] = backfill
SPEC.loader.exec_module(backfill)


class FakeKeycloakApi:
    def __init__(self, realms, clients_by_realm):
        self.realms = realms
        self.clients_by_realm = clients_by_realm
        self.created = []

    def list_realms(self):
        return [{"realm": realm} for realm in self.realms]

    def find_clients(self, realm, client_id):
        return [
            client
            for client in self.clients_by_realm.get(realm, [])
            if client.get("clientId") == client_id
        ]

    def create_client(self, realm, client):
        self.created.append((realm, client))
        return 201


class NonEnumeratingKeycloakApi(FakeKeycloakApi):
    def list_realms(self):
        raise AssertionError("targeted realm runs should not enumerate all realms")


class BackfillSonarWebappClientTest(unittest.TestCase):
    def test_builds_expected_client_with_defaults(self):
        config = self.config(airbyte_agents_url="https://app.airbyte.ai/")

        client = backfill.build_sonar_webapp_client(config)

        self.assertEqual("sonar-webapp", client["clientId"])
        self.assertEqual("Sonar Webapp", client["name"])
        self.assertEqual("openid-connect", client["protocol"])
        self.assertEqual("https://app.airbyte.ai", client["baseUrl"])
        self.assertEqual(["https://app.airbyte.ai/*"], client["redirectUris"])
        self.assertEqual(["https://app.airbyte.ai"], client["webOrigins"])
        self.assertTrue(client["enabled"])
        self.assertTrue(client["publicClient"])
        self.assertTrue(client["standardFlowEnabled"])
        self.assertFalse(client["directAccessGrantsEnabled"])
        self.assertFalse(client["serviceAccountsEnabled"])
        self.assertFalse(client["authorizationServicesEnabled"])
        self.assertFalse(client["frontchannelLogout"])
        self.assertFalse(client["implicitFlowEnabled"])

    def test_dry_run_records_missing_sonar_client_without_creating(self):
        api = FakeKeycloakApi(
            realms=["realm-a"],
            clients_by_realm={
                "realm-a": [{"clientId": "airbyte-webapp"}],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=True))

        self.assertEqual(1, summary.scanned)
        self.assertEqual(["realm-a"], summary.would_create)
        self.assertEqual([], api.created)
        self.assertFalse(summary.has_errors)

    def test_apply_creates_missing_sonar_client_in_sso_realm(self):
        api = FakeKeycloakApi(
            realms=["realm-a"],
            clients_by_realm={
                "realm-a": [{"clientId": "airbyte-webapp"}],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=False))

        self.assertEqual(["realm-a"], summary.created)
        self.assertEqual(1, len(api.created))
        self.assertEqual("realm-a", api.created[0][0])
        self.assertEqual("sonar-webapp", api.created[0][1]["clientId"])
        self.assertFalse(summary.has_errors)

    def test_skips_realms_without_airbyte_webapp(self):
        api = FakeKeycloakApi(realms=["master"], clients_by_realm={"master": []})

        summary = backfill.run_backfill(api, self.config(dry_run=False))

        self.assertEqual(["master"], summary.skipped_no_webapp)
        self.assertEqual([], api.created)
        self.assertFalse(summary.has_errors)

    def test_skips_existing_sonar_client_without_overwriting(self):
        api = FakeKeycloakApi(
            realms=["realm-a"],
            clients_by_realm={
                "realm-a": [
                    {"clientId": "airbyte-webapp"},
                    {"clientId": "sonar-webapp", "baseUrl": "https://different.example.com"},
                ],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=False))

        self.assertEqual(["realm-a"], summary.already_present)
        self.assertEqual([], api.created)
        self.assertFalse(summary.has_errors)

    def test_records_per_realm_errors_and_continues(self):
        class FailingCreateApi(FakeKeycloakApi):
            def create_client(self, realm, client):
                if realm == "bad-realm":
                    raise RuntimeError("boom")
                return super().create_client(realm, client)

        api = FailingCreateApi(
            realms=["bad-realm", "good-realm"],
            clients_by_realm={
                "bad-realm": [{"clientId": "airbyte-webapp"}],
                "good-realm": [{"clientId": "airbyte-webapp"}],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=False))

        self.assertEqual(["good-realm"], summary.created)
        self.assertIn("bad-realm", summary.errors)
        self.assertTrue(summary.has_errors)

    def test_concurrent_create_conflict_is_treated_as_already_present(self):
        class ConflictCreateApi(FakeKeycloakApi):
            def create_client(self, realm, client):
                self.clients_by_realm[realm].append({"clientId": "sonar-webapp"})
                raise backfill.KeycloakApiError("conflict", status=409)

        api = ConflictCreateApi(
            realms=["realm-a"],
            clients_by_realm={
                "realm-a": [{"clientId": "airbyte-webapp"}],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=False))

        self.assertEqual(["realm-a"], summary.already_present)
        self.assertFalse(summary.has_errors)

    def test_targeted_realm_run_only_processes_named_realms(self):
        api = NonEnumeratingKeycloakApi(
            realms=[],
            clients_by_realm={
                "realm-a": [{"clientId": "airbyte-webapp"}],
                "realm-b": [{"clientId": "airbyte-webapp"}],
                "realm-c": [{"clientId": "airbyte-webapp"}],
            },
        )

        summary = backfill.run_backfill(api, self.config(dry_run=False, realms=["realm-a", "realm-c"]))

        self.assertEqual(2, summary.scanned)
        self.assertEqual(["realm-a", "realm-c"], summary.created)
        self.assertEqual(["realm-a", "realm-c"], [realm for realm, _ in api.created])
        self.assertFalse(summary.has_errors)

    def config(
        self,
        dry_run=True,
        airbyte_url="https://cloud.airbyte.com",
        airbyte_agents_url="https://app.airbyte.ai",
        redirect_uris=None,
        web_origins=None,
        realms=None,
    ):
        return backfill.BackfillConfig(
            keycloak_base_url="http://localhost:8081/auth",
            bearer_token="token",
            dry_run=dry_run,
            airbyte_url=airbyte_url,
            airbyte_agents_url=airbyte_agents_url,
            redirect_uris=redirect_uris or [],
            web_origins=web_origins or [],
            realms=realms or [],
        )


if __name__ == "__main__":
    unittest.main()
