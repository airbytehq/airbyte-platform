#!/usr/bin/env bash

set -e

REGISTRY_URL="${AGENTS_CONNECTOR_REGISTRY_URL:-https://connectors.airbyte.ai/registry.json}"
OUTPUT_FILE="src/core/api/hooks/agentsSupportedSourceDefinitionIds.generated.ts"
if [[ "${AGENTS_SUPPORTED_DEFINITIONS_SKIP_FETCH:-}" == "true" && -f "$OUTPUT_FILE" ]]; then
  exit 0
fi
REGISTRY_FILE="$(mktemp)"
trap 'rm -f "$REGISTRY_FILE"' EXIT

# Set AGENTS_SUPPORTED_DEFINITIONS_STRICT=true to fail the build instead of
# writing an empty list when the registry is unavailable.
fail_or_fallback() {
  if [[ "${AGENTS_SUPPORTED_DEFINITIONS_STRICT:-}" == "true" ]]; then
    echo "Failed to load enabled connector definitions from $REGISTRY_URL" >&2
    exit 1
  fi
  echo "Warning: could not load connector registry from $REGISTRY_URL; writing empty Agents-supported list" >&2
  ids_json="[]"
}

if curl -fsS --retry 3 --retry-all-errors --connect-timeout 10 --max-time 60 "$REGISTRY_URL" >"$REGISTRY_FILE"; then
  if ! ids_json=$(jq -e '
    [.connectors | to_entries[]
      | select((.value.platform_availability.state // "enabled") == "enabled")
      | select(.value.runtime_mode != "direct_only")
      | .value.connector_definition_id]
    | sort
    | unique
    | if length == 0 then error("no enabled connector definitions found") else . end
  ' "$REGISTRY_FILE"); then
    fail_or_fallback
  fi
else
  fail_or_fallback
fi

{
  printf "// generated, do not change manually\n"
  printf "export const AGENTS_SUPPORTED_SOURCE_DEFINITION_IDS: readonly string[] = [\n"
  jq -r '.[] | "  " + (@json) + ","' <<<"$ids_json"
  printf "];\n"
} >"$OUTPUT_FILE"

if ! pnpm exec prettier --check "$OUTPUT_FILE"; then
  pnpm exec prettier --write "$OUTPUT_FILE"
fi
