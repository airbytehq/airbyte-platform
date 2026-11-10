import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { formatAge, getSecretStorageCardData } from "./utils";

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

  it("is neutral when none failed but one could not complete", () => {
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
