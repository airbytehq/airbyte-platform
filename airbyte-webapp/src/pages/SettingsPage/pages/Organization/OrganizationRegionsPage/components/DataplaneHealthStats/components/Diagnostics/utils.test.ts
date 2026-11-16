import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import {
  getSecretStorageCardData,
  getSecretStorageRows,
  getStoreName,
  getStoreScope,
  getWorkloadSchedulingCardData,
  getWorkloadSchedulingDetails,
} from "./utils";
import { mockDataplane, mockDiagnostic } from "../../DataplaneHealthStats.mocks";
import { formatAge, formatDuration, NEVER_CONNECTED } from "../../utils";

const diagnostic = (
  status: DataplaneDiagnosticStatus,
  overrides: Partial<DataplaneDiagnosticRead> = {}
): DataplaneDiagnosticRead => mockDiagnostic(DataplaneDiagnosticKind.SECRET_STORAGE, status, overrides);

const NOW = 1_000 + 30_000;

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead =>
  mockDataplane({ last_heartbeat_timestamp: NOW, ...overrides });

const PREFIX = "settings.organization.regions.dataplane.diagnostics.secretStorage";
const INCOMPLETE = "settings.organization.regions.dataplane.diagnostics.incomplete";

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

  it("returns nothing when the only result is that no secret store is configured", () => {
    const notConfigured = diagnostic(DataplaneDiagnosticStatus.NOT_CONFIGURED, {
      target_id: "dp-1",
      observation: { scope_type: "organization", scope_id: "org-1" },
    });
    expect(getSecretStorageCardData(dataplane({ diagnostics: [notConfigured] }), NOW)).toBeUndefined();
  });

  it("ignores a not configured result next to a store that passed", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.NOT_CONFIGURED, { target_id: "dp-1", checked_at: -100_000 }),
      diagnostic(DataplaneDiagnosticStatus.PASSED),
    ];
    expect(getSecretStorageCardData(dataplane({ diagnostics: stores }), NOW)).toEqual({
      level: "success",
      value: { messageId: `${PREFIX}.yes` },
      lastChecked: formatAge(30_000),
    });
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
      value: { messageId: INCOMPLETE },
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

  it("has no row for a result that no secret store is configured", () => {
    const notConfigured = diagnostic(DataplaneDiagnosticStatus.NOT_CONFIGURED, { target_id: "dp-1" });
    expect(getSecretStorageRows(dataplane({ diagnostics: [notConfigured] }), NOW)).toEqual([]);
  });

  it("still has a row for a store next to a not configured result", () => {
    const stores = [
      diagnostic(DataplaneDiagnosticStatus.NOT_CONFIGURED, { target_id: "dp-1" }),
      diagnostic(DataplaneDiagnosticStatus.PASSED, { target_id: "store-1" }),
    ];
    expect(getSecretStorageRows(dataplane({ diagnostics: stores }), NOW).map((row) => row.id)).toEqual(["store-1"]);
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

const WORKLOAD_PREFIX = "settings.organization.regions.dataplane.diagnostics.workloadScheduling";

const WORKLOAD_NOW = 1_000_000_000;
const WORKLOAD_MINUTE = 60_000;

const workloadDiagnostic = (
  status: DataplaneDiagnosticStatus,
  observation: DataplaneDiagnosticRead["observation"],
  overrides: Partial<DataplaneDiagnosticRead> = {}
): DataplaneDiagnosticRead =>
  mockDiagnostic(DataplaneDiagnosticKind.WORKLOAD_SCHEDULING, status, {
    target_id: "dp-1",
    checked_at: WORKLOAD_NOW - 30_000,
    updated_at: WORKLOAD_NOW,
    observation,
    ...overrides,
  });

const workloadDataplane = (diagnostics: DataplaneDiagnosticRead[] | undefined): DataplaneHealthRead =>
  mockDataplane({ last_heartbeat_timestamp: WORKLOAD_NOW, diagnostics });

// The longest blocked workload and the count, as the data plane reports them when something is blocked.
const blockedObservation = (overrides: DataplaneDiagnosticRead["observation"] = {}) => ({
  blocked_count: 1,
  longest_blocked_since: WORKLOAD_NOW - 7 * WORKLOAD_MINUTE,
  longest_blocked_workload_type: "SYNC",
  ...overrides,
});

const passedScan = workloadDiagnostic(DataplaneDiagnosticStatus.PASSED, {
  blocked_count: 0,
  last_scheduled: { scheduled_at: WORKLOAD_NOW - 2 * WORKLOAD_MINUTE, workload_type: "SYNC" },
});

describe("getWorkloadSchedulingCardData", () => {
  it("returns nothing when there is no workload scheduling result", () => {
    expect(getWorkloadSchedulingCardData(workloadDataplane([]), WORKLOAD_NOW)).toBeUndefined();
  });

  it("returns nothing when the diagnostics are missing", () => {
    expect(getWorkloadSchedulingCardData(workloadDataplane(undefined), WORKLOAD_NOW)).toBeUndefined();
  });

  it("returns nothing when only another check has a result", () => {
    const other = workloadDiagnostic(
      DataplaneDiagnosticStatus.PASSED,
      {},
      { kind: DataplaneDiagnosticKind.SECRET_STORAGE }
    );
    expect(getWorkloadSchedulingCardData(workloadDataplane([other]), WORKLOAD_NOW)).toBeUndefined();
  });

  it("returns nothing for a result that says nothing is configured", () => {
    const notConfigured = workloadDiagnostic(DataplaneDiagnosticStatus.NOT_CONFIGURED, {});
    expect(getWorkloadSchedulingCardData(workloadDataplane([notConfigured]), WORKLOAD_NOW)).toBeUndefined();
  });

  it("is never connected for a data plane without a check-in", () => {
    expect(
      getWorkloadSchedulingCardData(
        { ...workloadDataplane([passedScan]), last_heartbeat_timestamp: undefined },
        WORKLOAD_NOW
      )
    ).toEqual(NEVER_CONNECTED);
  });

  it("is a success with no workloads blocked when the scan passed", () => {
    expect(getWorkloadSchedulingCardData(workloadDataplane([passedScan]), WORKLOAD_NOW)).toEqual({
      level: "success",
      value: { messageId: `${WORKLOAD_PREFIX}.noneBlocked` },
      lastChecked: formatAge(30_000),
    });
  });

  it.each([1, 3])("is an error with the count when %s workloads are blocked", (count) => {
    const blocked = workloadDiagnostic(DataplaneDiagnosticStatus.FAILED, blockedObservation({ blocked_count: count }));
    expect(getWorkloadSchedulingCardData(workloadDataplane([blocked]), WORKLOAD_NOW)).toEqual({
      level: "error",
      value: { messageId: `${WORKLOAD_PREFIX}.blocked`, values: { count } },
      lastChecked: formatAge(30_000),
    });
  });

  it("is a warning when the scan could not complete", () => {
    const incomplete = workloadDiagnostic(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, {});
    expect(getWorkloadSchedulingCardData(workloadDataplane([incomplete]), WORKLOAD_NOW)).toEqual({
      level: "warning",
      value: { messageId: INCOMPLETE },
      lastChecked: formatAge(30_000),
    });
  });

  it("does not read a count from a scan that could not complete", () => {
    const incomplete = workloadDiagnostic(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, blockedObservation());
    expect(getWorkloadSchedulingCardData(workloadDataplane([incomplete]), WORKLOAD_NOW)?.value.messageId).toBe(
      INCOMPLETE
    );
  });
});

describe("getWorkloadSchedulingDetails", () => {
  it("returns nothing when there is no workload scheduling result", () => {
    expect(getWorkloadSchedulingDetails(workloadDataplane([]), WORKLOAD_NOW)).toBeUndefined();
  });

  it("shows nothing blocked and the last scheduled pod for a scan that passed", () => {
    expect(getWorkloadSchedulingDetails(workloadDataplane([passedScan]), WORKLOAD_NOW)).toEqual({
      blocked: { kind: "none" },
      lastScheduled: { workloadType: "SYNC", age: formatAge(2 * WORKLOAD_MINUTE) },
      lastProblem: undefined,
      error: undefined,
    });
  });

  it("names the longest blocked workload and how long it has waited", () => {
    const blocked = workloadDiagnostic(DataplaneDiagnosticStatus.FAILED, blockedObservation({ blocked_count: 3 }));
    expect(getWorkloadSchedulingDetails(workloadDataplane([blocked]), WORKLOAD_NOW)?.blocked).toEqual({
      kind: "blocked",
      count: 3,
      longest: { workloadType: "SYNC", waiting: formatDuration(7 * WORKLOAD_MINUTE) },
    });
  });

  it("reads the last scheduled pod and the last problem next to a blocked workload", () => {
    const blocked = workloadDiagnostic(
      DataplaneDiagnosticStatus.FAILED,
      blockedObservation({
        last_scheduled: { scheduled_at: WORKLOAD_NOW - WORKLOAD_MINUTE, workload_type: "CHECK" },
        last_problem: {
          observed_at: WORKLOAD_NOW - 5_000,
          reason: "Unschedulable",
          message: "0/3 nodes are available.",
        },
      })
    );
    const details = getWorkloadSchedulingDetails(workloadDataplane([blocked]), WORKLOAD_NOW);
    expect([details?.lastScheduled, details?.lastProblem]).toEqual([
      { workloadType: "CHECK", age: formatAge(WORKLOAD_MINUTE) },
      { message: "0/3 nodes are available.", age: formatAge(5_000) },
    ]);
  });

  it("keeps a problem that was observed before nothing was blocked", () => {
    const recovered = workloadDiagnostic(DataplaneDiagnosticStatus.PASSED, {
      blocked_count: 0,
      last_problem: {
        observed_at: WORKLOAD_NOW - 7 * WORKLOAD_MINUTE,
        reason: "Unschedulable",
        message: "0/3 nodes are available.",
      },
    });
    expect(getWorkloadSchedulingDetails(workloadDataplane([recovered]), WORKLOAD_NOW)).toMatchObject({
      blocked: { kind: "none" },
      lastProblem: { message: "0/3 nodes are available.", age: formatAge(7 * WORKLOAD_MINUTE) },
    });
  });

  it("has no blocked count, but keeps the earlier events and the error, when the scan could not complete", () => {
    const incomplete = workloadDiagnostic(
      DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
      {
        last_scheduled: { scheduled_at: WORKLOAD_NOW - 2 * WORKLOAD_MINUTE, workload_type: "SYNC" },
        last_problem: {
          observed_at: WORKLOAD_NOW - 7 * WORKLOAD_MINUTE,
          reason: "Unschedulable",
          message: "0/3 nodes are available.",
        },
      },
      {
        error_code: "ACCESS_DENIED",
        error_message: "The launcher is not allowed to list pods in its jobs namespace.",
      }
    );
    expect(getWorkloadSchedulingDetails(workloadDataplane([incomplete]), WORKLOAD_NOW)).toEqual({
      blocked: { kind: "notMeasured" },
      lastScheduled: { workloadType: "SYNC", age: formatAge(2 * WORKLOAD_MINUTE) },
      lastProblem: { message: "0/3 nodes are available.", age: formatAge(7 * WORKLOAD_MINUTE) },
      error: { code: "ACCESS_DENIED", message: "The launcher is not allowed to list pods in its jobs namespace." },
    });
  });

  it("shows the reason of the last problem when the scheduler gave no message", () => {
    const noMessage = workloadDiagnostic(DataplaneDiagnosticStatus.PASSED, {
      blocked_count: 0,
      last_problem: { observed_at: WORKLOAD_NOW - WORKLOAD_MINUTE, reason: "Unschedulable", message: "" },
    });
    expect(getWorkloadSchedulingDetails(workloadDataplane([noMessage]), WORKLOAD_NOW)?.lastProblem).toEqual({
      message: "Unschedulable",
      age: formatAge(WORKLOAD_MINUTE),
    });
  });

  it("keeps the error of a failed scan", () => {
    const blocked = workloadDiagnostic(DataplaneDiagnosticStatus.FAILED, blockedObservation(), {
      error_code: "UNSCHEDULABLE",
      error_message: "A workload cannot be scheduled.",
    });
    expect(getWorkloadSchedulingDetails(workloadDataplane([blocked]), WORKLOAD_NOW)?.error).toEqual({
      code: "UNSCHEDULABLE",
      message: "A workload cannot be scheduled.",
    });
  });

  it("has no error when the code or the message is missing", () => {
    const withCodeOnly = workloadDiagnostic(
      DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
      {},
      { error_code: "ACCESS_DENIED" }
    );
    expect(getWorkloadSchedulingDetails(workloadDataplane([withCodeOnly]), WORKLOAD_NOW)?.error).toBeUndefined();
  });

  describe("when the observation is not what the data plane documents", () => {
    it.each([
      ["has no observation", undefined],
      ["has an event that is not an object", { last_scheduled: "now", last_problem: 5 }],
      [
        "has an event time that is not epoch milliseconds",
        { last_scheduled: { scheduled_at: "2026-11-12T10:00:00Z", workload_type: "SYNC" } },
      ],
      ["has an event time of zero", { last_problem: { observed_at: 0, reason: "Unschedulable" } }],
      [
        "has an event without its text",
        {
          last_scheduled: { scheduled_at: WORKLOAD_NOW - WORKLOAD_MINUTE },
          last_problem: { observed_at: WORKLOAD_NOW },
        },
      ],
    ])("reads no events when it %s", (_description, observation) => {
      const unreadable = workloadDiagnostic(DataplaneDiagnosticStatus.PASSED, observation);
      const details = getWorkloadSchedulingDetails(workloadDataplane([unreadable]), WORKLOAD_NOW);
      expect([details?.lastScheduled, details?.lastProblem]).toEqual([undefined, undefined]);
    });
  });
});
