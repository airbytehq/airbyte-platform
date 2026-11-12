import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { getSecretStorageCardData, getSecretStorageRows, getStoreName, getStoreScope } from "./utils";
import { formatAge } from "../../utils";

const diagnostic = (
  status: DataplaneDiagnosticStatus,
  overrides: Partial<DataplaneDiagnosticRead> = {}
): DataplaneDiagnosticRead => ({
  kind: DataplaneDiagnosticKind.SECRET_STORAGE,
  target_id: "store-1",
  status,
  checked_at: 1_000,
  updated_at: 1_000,
  ...overrides,
});

const NOW = 1_000 + 30_000;

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  last_heartbeat_timestamp: NOW,
  ...overrides,
});
const PREFIX = "settings.organization.regions.dataplane.diagnostics.secretStorage";

describe("getSecretStorageCardData", () => {
  it("returns nothing when there are no diagnostics", () => {
    expect(getSecretStorageCardData(dataplane({ diagnostics: [] }), NOW)).toBeUndefined();
  });

  it("returns nothing when the diagnostics are missing", () => {
    expect(getSecretStorageCardData(dataplane({ diagnostics: undefined }), NOW)).toBeUndefined();
  });

  it("returns nothing when no diagnostic is a secret store result", () => {
    const otherKind = diagnostic(DataplaneDiagnosticStatus.FAILED, { kind: "OTHER" as DataplaneDiagnosticKind });
    expect(getSecretStorageCardData(dataplane({ diagnostics: [otherKind] }), NOW)).toBeUndefined();
  });

  it("is a success when every store passed", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.PASSED),
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-2" }),
    ];
    expect(getSecretStorageCardData(dataplane({ diagnostics: stores }), NOW)).toEqual({
      level: "success",
      value: { messageId: `${PREFIX}.yes` },
      lastChecked: formatAge(30_000),
    });
  });

  it("is an error when any store failed, even if another could not complete", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE),
      diagnostic(DataplaneDiagnosticStatus.FAILED, { target_id: "store-2" }),
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-3" }),
    ];
    expect(getSecretStorageCardData(dataplane({ diagnostics: stores }), NOW)).toEqual({
      level: "error",
      value: { messageId: `${PREFIX}.no` },
      lastChecked: formatAge(30_000),
    });
  });

  it("is a warning when none failed but one could not complete", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.PASSED),
      diagnostic(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, { target_id: "store-2" }),
    ];
    expect(getSecretStorageCardData(dataplane({ diagnostics: stores }), NOW)).toEqual({
      level: "warning",
      value: { messageId: `${PREFIX}.incomplete` },
      lastChecked: formatAge(30_000),
    });
  });

  it("reports the age of the oldest result", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.PASSED, { checked_at: 20_000 }),
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-2", checked_at: 1_000 }),
    ];
    expect(getSecretStorageCardData(dataplane({ diagnostics: stores }), NOW)?.lastChecked).toEqual(formatAge(30_000));
  });

  it("is neutral and never connected for a data plane without a check-in", () => {
    const neverConnected = dataplane({ last_heartbeat_timestamp: undefined });
    expect(getSecretStorageCardData(neverConnected, NOW)).toEqual({
      level: "neutral",
      value: { messageId: "settings.organization.regions.neverConnected" },
    });
  });
});

describe("getStoreScope", () => {
  it("reads a workspace scope", () => {
    expect(getStoreScope({ scope_type: "workspace", scope_id: "workspace-1" })).toEqual({
      type: "workspace",
      id: "workspace-1",
    });
  });

  it("reads an organization scope", () => {
    expect(getStoreScope({ scope_type: "organization", scope_id: "org-1" })).toEqual({
      type: "organization",
      id: "org-1",
    });
  });

  it("reads a scope type in any letter case", () => {
    expect(getStoreScope({ scope_type: "ORGANIZATION", scope_id: "org-1" })).toEqual({
      type: "organization",
      id: "org-1",
    });
  });

  it("is unknown without an observation", () => {
    expect(getStoreScope(undefined)).toBeUndefined();
  });

  it("is unknown for a scope type it does not know", () => {
    expect(getStoreScope({ scope_type: "CONNECTION", scope_id: "connection-1" })).toBeUndefined();
  });

  it("is unknown when the scope id is not a string", () => {
    expect(getStoreScope({ scope_type: "WORKSPACE", scope_id: 42 })).toBeUndefined();
  });

  it("is unknown when the scope type is missing", () => {
    expect(getStoreScope({ scope_id: "workspace-1" })).toBeUndefined();
  });
});

describe("getStoreName", () => {
  it("reads the name next to the store id", () => {
    expect(getStoreName(diagnostic(DataplaneDiagnosticStatus.PASSED, { target_name: "Acme vault" }))).toBe(
      "Acme vault"
    );
  });

  it("is unknown when the diagnostic has no name", () => {
    expect(getStoreName(diagnostic(DataplaneDiagnosticStatus.PASSED))).toBeUndefined();
  });

  it("is unknown when the name is empty", () => {
    expect(getStoreName(diagnostic(DataplaneDiagnosticStatus.PASSED, { target_name: "" }))).toBeUndefined();
  });

  it("is unknown when the name is not a string", () => {
    const malformed = {
      ...diagnostic(DataplaneDiagnosticStatus.PASSED),
      target_name: null,
    } as unknown as DataplaneDiagnosticRead;
    expect(getStoreName(malformed)).toBeUndefined();
  });
});

describe("getSecretStorageRows", () => {
  const observation = { scope_type: "workspace", scope_id: "workspace-1" };

  it("has no rows without diagnostics", () => {
    expect(getSecretStorageRows(dataplane({ diagnostics: undefined }), NOW)).toEqual([]);
  });

  it("ignores a diagnostic that is not a secret store result", () => {
    const otherKind = diagnostic(DataplaneDiagnosticStatus.PASSED, { kind: "OTHER" as DataplaneDiagnosticKind });
    expect(getSecretStorageRows(dataplane({ diagnostics: [otherKind] }), NOW)).toEqual([]);
  });

  it("has one row for each store, in the order they were reported", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-1" }),
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-2" }),
    ];
    expect(getSecretStorageRows(dataplane({ diagnostics: stores }), NOW).map((row) => row.id)).toEqual([
      "store-1",
      "store-2",
    ]);
  });

  it("gives a store that passed the success level and no error", () => {
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [diagnostic(DataplaneDiagnosticStatus.PASSED)] }), NOW);
    expect(row).toMatchObject({ level: "success", error: undefined });
  });

  it("gives a store that failed the error level and its error", () => {
    const failed = diagnostic(DataplaneDiagnosticStatus.FAILED, {
      error_code: "ACCESS_DENIED",
      error_message: "Resource policy denied the secret read.",
    });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [failed] }), NOW);
    expect(row).toMatchObject({
      level: "error",
      error: { code: "ACCESS_DENIED", message: "Resource policy denied the secret read." },
    });
  });

  it("gives a store that could not complete the warning level and its error", () => {
    const incomplete = diagnostic(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, {
      error_code: "TIMEOUT",
      error_message: "The secret read did not complete.",
    });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [incomplete] }), NOW);
    expect(row).toMatchObject({
      level: "warning",
      error: { code: "TIMEOUT", message: "The secret read did not complete." },
    });
  });

  it("has no error when the code or the message is missing", () => {
    const failed = diagnostic(DataplaneDiagnosticStatus.FAILED, { error_code: "ACCESS_DENIED" });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [failed] }), NOW);
    expect(row.error).toBeUndefined();
  });

  it("reads the scope from the observation", () => {
    const store = diagnostic(DataplaneDiagnosticStatus.PASSED, { observation });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [store] }), NOW);
    expect(row.scope).toEqual({ type: "workspace", id: "workspace-1" });
  });

  it("has an unknown scope when the observation is not understood", () => {
    const store = diagnostic(DataplaneDiagnosticStatus.PASSED, { observation: { scope_type: "SOMETHING" } });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [store] }), NOW);
    expect(row.scope).toBeUndefined();
  });

  it("gives the age of the store's own check", () => {
    const store = diagnostic(DataplaneDiagnosticStatus.PASSED, { checked_at: NOW - 90_000 });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [store] }), NOW);
    expect(row.lastChecked).toEqual(formatAge(90_000));
  });

  it("gives a store the name reported for it", () => {
    const store = diagnostic(DataplaneDiagnosticStatus.PASSED, { target_name: "Acme vault" });
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [store] }), NOW);
    expect(row.name).toBe("Acme vault");
  });

  it("leaves a store without a name when none is reported", () => {
    const [row] = getSecretStorageRows(dataplane({ diagnostics: [diagnostic(DataplaneDiagnosticStatus.PASSED)] }), NOW);
    expect(row.name).toBeUndefined();
  });
});
