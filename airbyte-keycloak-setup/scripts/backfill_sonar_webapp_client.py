#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# ///

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


AIRBYTE_WEBAPP_CLIENT_ID = "airbyte-webapp"
SONAR_WEBAPP_CLIENT_ID = "sonar-webapp"
SONAR_WEBAPP_CLIENT_NAME = "Sonar Webapp"
REALM_ENUMERATION_ERROR_KEY = "<realm-enumeration>"
DEFAULT_KEYCLOAK_BASE_URL = "http://localhost:8081/auth"
DEFAULT_AIRBYTE_URL = "https://cloud.airbyte.com"


@dataclass
class BackfillConfig:
    keycloak_base_url: str
    bearer_token: str
    dry_run: bool
    airbyte_url: str
    airbyte_agents_url: str
    redirect_uris: list[str]
    web_origins: list[str]
    realms: list[str]


@dataclass
class BackfillSummary:
    dry_run: bool
    scanned: int = 0
    created: list[str] = field(default_factory=list)
    would_create: list[str] = field(default_factory=list)
    already_present: list[str] = field(default_factory=list)
    skipped_no_webapp: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    def render(self) -> str:
        lines = [
            "sonar-webapp client backfill complete "
            f"(dry_run={self.dry_run}, scanned={self.scanned}, "
            f"created={len(self.created)}, would_create={len(self.would_create)}, "
            f"already_present={len(self.already_present)}, "
            f"skipped_no_webapp={len(self.skipped_no_webapp)}, errors={len(self.errors)})"
        ]
        lines.extend(render_named_list("created", self.created))
        lines.extend(render_named_list("would_create", self.would_create))
        lines.extend(render_named_list("already_present", self.already_present))
        lines.extend(render_named_list("skipped_no_webapp", self.skipped_no_webapp))
        for realm, message in sorted(self.errors.items()):
            lines.append(f"error {realm}: {message}")
        return "\n".join(lines)


class KeycloakApiError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class KeycloakAdminApi:
    def __init__(self, base_url: str, bearer_token: str, timeout_seconds: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.timeout_seconds = timeout_seconds

    def list_realms(self) -> list[dict[str, Any]]:
        return self._request_json("GET", "/admin/realms")

    def find_clients(self, realm: str, client_id: str) -> list[dict[str, Any]]:
        query = urlencode({"clientId": client_id})
        return self._request_json("GET", f"/admin/realms/{quote(realm, safe='')}/clients?{query}")

    def create_client(self, realm: str, client: dict[str, Any]) -> int:
        return self._request_status("POST", f"/admin/realms/{quote(realm, safe='')}/clients", client)

    def _request_json(self, method: str, path: str) -> Any:
        status, body = self._request(method, path, None)
        if not 200 <= status < 300:
            raise KeycloakApiError(f"{method} {path} returned HTTP {status}: {body}", status)
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise KeycloakApiError(f"{method} {path} returned invalid JSON: {exc}") from exc

    def _request_status(self, method: str, path: str, payload: dict[str, Any]) -> int:
        status, body = self._request(method, path, payload)
        if not 200 <= status < 300:
            raise KeycloakApiError(f"{method} {path} returned HTTP {status}: {body}", status)
        return status

    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]]) -> tuple[int, str]:
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = response.read().decode("utf-8", errors="replace")
                return response.status, response_body
        except HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            return exc.code, response_body
        except URLError as exc:
            raise KeycloakApiError(f"{method} {path} failed: {exc}") from exc


def run_backfill(api: Any, config: BackfillConfig) -> BackfillSummary:
    summary = BackfillSummary(dry_run=config.dry_run)
    expected_client = build_sonar_webapp_client(config)
    logging.info("sonar-webapp client backfill starting (dry_run=%s)", config.dry_run)

    if config.realms:
        realms = without_blank_values(config.realms)
        logging.info("Restricting backfill to %s configured realm(s): %s", len(realms), ", ".join(realms))
    else:
        try:
            realm_representations = api.list_realms()
        except Exception as exc:
            logging.exception("Unable to enumerate Keycloak realms")
            summary.errors[REALM_ENUMERATION_ERROR_KEY] = str(exc) or exc.__class__.__name__
            return summary

        realms = [
            str(realm_representation.get("realm") or realm_representation.get("id") or "").strip()
            for realm_representation in realm_representations
        ]

    for realm in realms:
        if not realm:
            continue
        process_realm(api, config, summary, realm, expected_client)

    return summary


def process_realm(
    api: Any,
    config: BackfillConfig,
    summary: BackfillSummary,
    realm: str,
    expected_client: dict[str, Any],
) -> None:
    summary.scanned += 1
    try:
        existing_sonar_clients = api.find_clients(realm, SONAR_WEBAPP_CLIENT_ID)
        if existing_sonar_clients:
            log_drift_if_any(realm, expected_client, existing_sonar_clients[0])
            summary.already_present.append(realm)
            logging.info("Realm %r already has the %s client; skipping", realm, SONAR_WEBAPP_CLIENT_ID)
            return

        existing_airbyte_clients = api.find_clients(realm, AIRBYTE_WEBAPP_CLIENT_ID)
        if not existing_airbyte_clients:
            summary.skipped_no_webapp.append(realm)
            logging.info("Skipping realm %r: no %s client", realm, AIRBYTE_WEBAPP_CLIENT_ID)
            return

        if config.dry_run:
            summary.would_create.append(realm)
            logging.info("[dry-run] Would create %s client in realm %r", SONAR_WEBAPP_CLIENT_ID, realm)
            return

        try:
            status = api.create_client(realm, expected_client)
        except KeycloakApiError as exc:
            if exc.status == 409 and api.find_clients(realm, SONAR_WEBAPP_CLIENT_ID):
                summary.already_present.append(realm)
                logging.info(
                    "Realm %r already has the %s client after a concurrent create; skipping",
                    realm,
                    SONAR_WEBAPP_CLIENT_ID,
                )
                return
            raise
        summary.created.append(realm)
        logging.info("Created %s client in realm %r. Status: %s", SONAR_WEBAPP_CLIENT_ID, realm, status)
    except Exception as exc:
        logging.exception("Error backfilling %s client in realm %r", SONAR_WEBAPP_CLIENT_ID, realm)
        summary.errors[realm] = str(exc) or exc.__class__.__name__


def build_sonar_webapp_client(config: BackfillConfig) -> dict[str, Any]:
    base_url = (config.airbyte_agents_url or config.airbyte_url).rstrip("/")
    redirect_uris = without_blank_values(config.redirect_uris) or [f"{base_url}/*"]
    web_origins = without_blank_values(config.web_origins) or [base_url]
    return {
        "clientId": SONAR_WEBAPP_CLIENT_ID,
        "name": SONAR_WEBAPP_CLIENT_NAME,
        "protocol": "openid-connect",
        "redirectUris": redirect_uris,
        "webOrigins": web_origins,
        "baseUrl": base_url,
        "enabled": True,
        "directAccessGrantsEnabled": False,
        "standardFlowEnabled": True,
        "serviceAccountsEnabled": False,
        "authorizationServicesEnabled": False,
        "frontchannelLogout": False,
        "implicitFlowEnabled": False,
        "publicClient": True,
    }


def log_drift_if_any(realm: str, expected: dict[str, Any], existing: dict[str, Any]) -> None:
    drifted_fields = []
    for field_name, expected_value in expected.items():
        existing_value = normalize_client_value(field_name, existing.get(field_name))
        normalized_expected = normalize_client_value(field_name, expected_value)
        if existing_value != normalized_expected:
            drifted_fields.append(field_name)

    if drifted_fields:
        logging.warning(
            "Realm %r already has the %s client but its config differs in: %s. "
            "Leaving it unchanged.",
            realm,
            SONAR_WEBAPP_CLIENT_ID,
            ", ".join(drifted_fields),
        )


def normalize_client_value(field_name: str, value: Any) -> Any:
    if field_name in {
        "enabled",
        "directAccessGrantsEnabled",
        "standardFlowEnabled",
        "serviceAccountsEnabled",
        "authorizationServicesEnabled",
        "frontchannelLogout",
        "implicitFlowEnabled",
        "publicClient",
    }:
        return bool(value)
    if field_name in {"redirectUris", "webOrigins"}:
        return value or []
    return value


def render_named_list(name: str, values: list[str]) -> list[str]:
    if not values:
        return []
    return [f"{name}: {', '.join(values)}"]


def without_blank_values(values: list[str]) -> list[str]:
    return [value.strip() for value in values if value.strip()]


def parse_list_env(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None or value == "":
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def default_keycloak_base_url() -> str:
    explicit = os.getenv("KEYCLOAK_BASE_URL")
    if explicit:
        return explicit

    host = os.getenv("KEYCLOAK_INTERNAL_HOST")
    if not host:
        return DEFAULT_KEYCLOAK_BASE_URL

    protocol = os.getenv("KEYCLOAK_PROTOCOL", "http")
    base_path = os.getenv("KEYCLOAK_BASE_PATH", "/auth")
    return f"{protocol}://{host.rstrip('/')}/{base_path.strip('/')}"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill the sonar-webapp Keycloak client into existing SSO realms.",
    )
    parser.add_argument(
        "--keycloak-base-url",
        default=default_keycloak_base_url(),
        help=f"Keycloak base URL including /auth. Defaults to {DEFAULT_KEYCLOAK_BASE_URL}, "
        "or KEYCLOAK_BASE_URL / KEYCLOAK_PROTOCOL + KEYCLOAK_INTERNAL_HOST + KEYCLOAK_BASE_PATH.",
    )
    parser.add_argument(
        "--bearer-token",
        default=os.getenv("KEYCLOAK_BEARER_TOKEN"),
        help="Keycloak admin bearer token. Can also be set with KEYCLOAK_BEARER_TOKEN.",
    )
    parser.add_argument(
        "--airbyte-url",
        default=os.getenv("AIRBYTE_URL", DEFAULT_AIRBYTE_URL),
        help=f"Airbyte webapp URL used as fallback for the Sonar client. Defaults to {DEFAULT_AIRBYTE_URL}.",
    )
    parser.add_argument(
        "--airbyte-agents-url",
        "--sonar-webapp-url",
        dest="airbyte_agents_url",
        default=os.getenv("AIRBYTE_AGENTS_URL", ""),
        help="Sonar webapp base URL. Defaults to AIRBYTE_AGENTS_URL, falling back to --airbyte-url.",
    )
    parser.add_argument(
        "--redirect-uri",
        dest="redirect_uris",
        action="append",
        default=None,
        help="Allowed redirect URI for sonar-webapp. Repeat for multiple values. "
        "Defaults to AIRBYTE_AGENTS_VALID_REDIRECT_URIS, then <sonar-url>/*.",
    )
    parser.add_argument(
        "--web-origin",
        dest="web_origins",
        action="append",
        default=None,
        help="Allowed web origin for sonar-webapp. Repeat for multiple values. "
        "Defaults to AIRBYTE_AGENTS_WEB_ORIGINS, then <sonar-url>.",
    )
    parser.add_argument(
        "--realm",
        dest="realms",
        action="append",
        default=None,
        help="Only process this Keycloak realm. Repeat for a controlled spot-check set. "
        "Defaults to all realms returned by Keycloak.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("KEYCLOAK_BACKFILL_TIMEOUT_SECONDS", "30")),
        help="HTTP timeout per Keycloak API request.",
    )
    try:
        dry_run_default = parse_bool(os.getenv("KEYCLOAK_BACKFILL_SONAR_WEBAPP_CLIENT_DRY_RUN"), True)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    dry_run_group = parser.add_mutually_exclusive_group()
    dry_run_group.add_argument("--dry-run", dest="dry_run", action="store_true", help="Print changes without applying them.")
    dry_run_group.add_argument("--apply", dest="dry_run", action="store_false", help="Create missing clients.")
    parser.set_defaults(dry_run=dry_run_default)
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")

    if not args.bearer_token:
        logging.error("Provide a Keycloak bearer token with --bearer-token or KEYCLOAK_BEARER_TOKEN.")
        return 2

    config = BackfillConfig(
        keycloak_base_url=args.keycloak_base_url,
        bearer_token=args.bearer_token,
        dry_run=args.dry_run,
        airbyte_url=args.airbyte_url,
        airbyte_agents_url=args.airbyte_agents_url,
        redirect_uris=args.redirect_uris
        if args.redirect_uris is not None
        else parse_list_env(os.getenv("AIRBYTE_AGENTS_VALID_REDIRECT_URIS")),
        web_origins=args.web_origins
        if args.web_origins is not None
        else parse_list_env(os.getenv("AIRBYTE_AGENTS_WEB_ORIGINS")),
        realms=args.realms or [],
    )
    api = KeycloakAdminApi(config.keycloak_base_url, config.bearer_token, args.timeout_seconds)
    summary = run_backfill(api, config)

    if summary.has_errors:
        logging.warning(summary.render())
    else:
        logging.info(summary.render())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
