import { renderHook } from "@testing-library/react-hooks";
import dayjs from "dayjs";

import { Status as ConnectionSyncStatus } from "components/EntityTable/types";
import { mockConnection } from "test-utils";

import {
  ConnectionScheduleDataBasicSchedule,
  ConnectionScheduleDataCron,
  ConnectionStatus,
  SchemaChange,
} from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { isConnectionLate, isHandleableScheduledConnection, useConnectionStatus } from "./useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";
import { useConnectionSyncContext } from "../ConnectionSync/ConnectionSyncContext";

const MULTIPLIER_EXPERIMENT_VALUE = 2;

jest.mock("hooks/connection/useSchemaChanges");
type RequiredSchemaChangesShape = Pick<ReturnType<typeof useSchemaChanges>, "hasBreakingSchemaChange">;
const mockUseSchemaChanges = useSchemaChanges as unknown as jest.Mock<RequiredSchemaChangesShape>;

jest.mock("components/connection/StreamStatus/streamStatusUtils", () => ({
  useLateMultiplierExperiment: () => {
    return MULTIPLIER_EXPERIMENT_VALUE;
  },
  useErrorMultiplierExperiment: () => {
    return MULTIPLIER_EXPERIMENT_VALUE;
  },
}));

jest.mock("hooks/services/ConnectionEdit/ConnectionEditService");
interface RequiredConnectionShape {
  connection: Pick<
    ReturnType<typeof useConnectionEditService>["connection"],
    "status" | "schemaChange" | "scheduleType" | "scheduleData"
  >;
}
const mockUseConnectionEditService = useConnectionEditService as unknown as jest.Mock<RequiredConnectionShape>;

jest.mock("../ConnectionSync/ConnectionSyncContext");
type RequiredConnectionSyncShape = Pick<
  ReturnType<typeof useConnectionSyncContext>,
  "connectionStatus" | "lastSuccessfulSync"
>;
const mockUseConnectionSyncContext = useConnectionSyncContext as unknown as jest.Mock<RequiredConnectionSyncShape>;

interface MockSetup {
  // connection values
  connectionStatus: RequiredConnectionShape["connection"]["status"];
  schemaChange: RequiredConnectionShape["connection"]["schemaChange"];
  scheduleType?: RequiredConnectionShape["connection"]["scheduleType"];
  scheduleData?: RequiredConnectionShape["connection"]["scheduleData"];

  // auxilliary
  connectionSyncStatus: RequiredConnectionSyncShape["connectionStatus"];
  lastSuccessfulSync: RequiredConnectionSyncShape["lastSuccessfulSync"];
}
const resetAndSetupMocks = ({
  connectionStatus,
  schemaChange,
  scheduleType,
  scheduleData,
  connectionSyncStatus,
  lastSuccessfulSync,
}: MockSetup) => {
  // 👻 re-implementing this logic from useConnectionSyncContextInit which we're avoiding
  // via mocks, but it's better than making each test case redefine this computed value
  const connectionEnabled = connectionStatus === ConnectionStatus.active;

  if (schemaChange === SchemaChange.breaking && connectionStatus !== ConnectionStatus.inactive) {
    // platform disables a connection when there is a breaking schema change
    throw new Error("A breaking schema change should always result in an inactive connection");
  }

  const hasBreakingSchemaChange = schemaChange === SchemaChange.breaking;

  mockUseConnectionEditService.mockImplementation(() => ({
    connection: {
      status: connectionStatus,
      schemaChange,
      scheduleType,
      scheduleData,
    },
  }));

  mockUseConnectionSyncContext.mockImplementation(() => ({
    connectionEnabled: hasBreakingSchemaChange ? false : connectionEnabled,
    connectionStatus: connectionSyncStatus,
    lastSuccessfulSync,
  }));

  mockUseSchemaChanges.mockImplementation(() => ({
    hasBreakingSchemaChange,
  }));
};

describe("isConnectionLate", () => {
  describe("manual syncs", () => {
    it("is not late when there is no last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "manual",
          },
          undefined,
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when there is a recent last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "manual",
          },
          dayjs().subtract(1, "hour").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when there is a distant last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "manual",
          },
          dayjs().subtract(1, "month").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
  });

  describe("cron schedule", () => {
    it("is not late when there is no last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "cron",
            scheduleData: { cron: { cronExpression: "* * * * *", cronTimeZone: "UTC" } },
          },
          undefined,
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when there is a recent last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "cron",
            scheduleData: { cron: { cronExpression: "* * * * *", cronTimeZone: "UTC" } },
          },
          dayjs().subtract(1, "hour").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when there is a distant last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "cron",
            scheduleData: { cron: { cronExpression: "* * * * *", cronTimeZone: "UTC" } },
          },
          dayjs().subtract(1, "month").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
  });

  describe("basic schedule", () => {
    it("is not late when there is no last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "basic",
            scheduleData: { basicSchedule: { units: 24, timeUnit: "hours" } },
          },
          undefined,
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when there is a recent last successful sync", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "basic",
            scheduleData: { basicSchedule: { units: 24, timeUnit: "hours" } },
          },
          dayjs().subtract(1, "hour").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is not late when the last successful sync is within 1x<->2x frequency", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "basic",
            scheduleData: { basicSchedule: { units: 24, timeUnit: "hours" } },
          },
          dayjs().subtract(25, "hour").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(false);
    });
    it("is late when the last successful sync is beyond the 2x frequency", () => {
      expect(
        isConnectionLate(
          {
            ...mockConnection,
            scheduleType: "basic",
            scheduleData: { basicSchedule: { units: 24, timeUnit: "hours" } },
          },
          dayjs().subtract(50, "hour").unix(),
          MULTIPLIER_EXPERIMENT_VALUE
        )
      ).toBe(true);
    });
  });
});

describe("isHandleableScheduledConnection", () => {
  it("returns false when no connection schedule exists", () => {
    expect(isHandleableScheduledConnection(undefined)).toBe(false);
  });

  it("returns false when connection schedule is manual", () => {
    expect(isHandleableScheduledConnection("manual")).toBe(false);
  });

  it("returns false when connection schedule is cron", () => {
    expect(isHandleableScheduledConnection("cron")).toBe(false);
  });

  it("returns true when connection schedule is basic", () => {
    expect(isHandleableScheduledConnection("basic")).toBe(true);
  });
});

describe("useConnectionStatus", () => {
  const within1xFrequency = dayjs().subtract(20, "hours").unix();
  const within2xFrequency = dayjs().subtract(28, "hours").unix();
  const outside2xFrequency = dayjs().subtract(50, "hours").unix();

  it.each`
    title                                                                                           | expectedConnectionStatus                          | connectionStatus             | schemaChange                 | connectionSyncStatus              | lastSuccessfulSync    | scheduleType | scheduleData
    ${"most recent sync was successful, no schema changes"}                                         | ${ConnectionStatusIndicatorStatus.OnTime}         | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.ACTIVE}    | ${within1xFrequency}  | ${undefined} | ${undefined}
    ${"most recent sync was successful, breaking schema changes"}                                   | ${ConnectionStatusIndicatorStatus.ActionRequired} | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${ConnectionSyncStatus.ACTIVE}    | ${within1xFrequency}  | ${undefined} | ${undefined}
    ${"breaking schema changes, sync is within 1x frequency"}                                       | ${ConnectionStatusIndicatorStatus.ActionRequired} | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${ConnectionSyncStatus.FAILED}    | ${within1xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"breaking schema changes, sync is within 2x frequency"}                                       | ${ConnectionStatusIndicatorStatus.ActionRequired} | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${ConnectionSyncStatus.FAILED}    | ${within2xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"breaking schema changes, sync is outside of 2x frequency"}                                   | ${ConnectionStatusIndicatorStatus.ActionRequired} | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${ConnectionSyncStatus.FAILED}    | ${outside2xFrequency} | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"new connection, not scheduled"}                                                              | ${ConnectionStatusIndicatorStatus.Pending}        | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.EMPTY}     | ${undefined}          | ${undefined} | ${undefined}
    ${"new connection, scheduled"}                                                                  | ${ConnectionStatusIndicatorStatus.Pending}        | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.EMPTY}     | ${undefined}          | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"connection status is failed, no previous success"}                                           | ${ConnectionStatusIndicatorStatus.Error}          | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.FAILED}    | ${undefined}          | ${undefined} | ${undefined}
    ${"connection status is failed, last previous success was within 1x schedule frequency"}        | ${ConnectionStatusIndicatorStatus.OnTrack}        | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.FAILED}    | ${within1xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"connection status is failed, last previous success was within 2x schedule frequency"}        | ${ConnectionStatusIndicatorStatus.OnTrack}        | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.FAILED}    | ${within2xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"connection status is failed, last previous success was within 2x schedule frequency (cron)"} | ${ConnectionStatusIndicatorStatus.Error}          | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.FAILED}    | ${within2xFrequency}  | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"connection status is failed, last previous success was outside 2x schedule frequency"}       | ${ConnectionStatusIndicatorStatus.Error}          | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${ConnectionSyncStatus.FAILED}    | ${outside2xFrequency} | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"connection status is failed, last previous success was within 2x schedule frequency"}        | ${ConnectionStatusIndicatorStatus.OnTrack}        | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.ACTIVE}    | ${within2xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync was successful, but the next sync hasn't started (outside 2x frequency)"}          | ${ConnectionStatusIndicatorStatus.Late}           | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.ACTIVE}    | ${outside2xFrequency} | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync was successful, but the next sync hasn't started (outside 2x frequency, cron)"}    | ${ConnectionStatusIndicatorStatus.OnTime}         | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.ACTIVE}    | ${outside2xFrequency} | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"last sync was cancelled, but the next cron-scheduled sync hasn't started"}                   | ${ConnectionStatusIndicatorStatus.OnTime}         | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.CANCELLED} | ${within1xFrequency}  | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"last sync was cancelled, but last successful sync is within 1x frequency"}                   | ${ConnectionStatusIndicatorStatus.OnTime}         | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.CANCELLED} | ${within1xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync was cancelled, but last successful sync is within 2x frequency"}                   | ${ConnectionStatusIndicatorStatus.OnTrack}        | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.CANCELLED} | ${within2xFrequency}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync was cancelled, but last successful sync is outside 2x frequency"}                  | ${ConnectionStatusIndicatorStatus.Late}           | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${ConnectionSyncStatus.CANCELLED} | ${outside2xFrequency} | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
  `(
    "$title:" +
      "\n\treturns $expectedConnectionStatus when" +
      "\n\ta connection's status is $connectionStatus" +
      "\n\ta connection's last completed sync status is $connectionSyncStatus" +
      "\n\twith a $scheduleType schedule: $scheduleData" +
      "\n\tand has $schemaChange schema changes",
    ({ expectedConnectionStatus, scheduleType, scheduleData, ...mockConfig }) => {
      resetAndSetupMocks({ ...mockConfig, ...buildScheduleData(scheduleType, scheduleData) });
      const { result } = renderHook(() => useConnectionStatus());
      expect(result.current).toBe(expectedConnectionStatus);
    }
  );
});

type ScheduleData = Pick<RequiredConnectionShape["connection"], "scheduleType" | "scheduleData">;
function buildScheduleData(type: "manual", schedule?: undefined): ScheduleData;
function buildScheduleData(type: "basic", schedule: ConnectionScheduleDataBasicSchedule): ScheduleData;
function buildScheduleData(type: "cron", schedule: ConnectionScheduleDataCron): ScheduleData;
function buildScheduleData(
  scheduleType: RequiredConnectionShape["connection"]["scheduleType"],
  schedule: ConnectionScheduleDataBasicSchedule | ConnectionScheduleDataCron | undefined
): ScheduleData {
  if (scheduleType === "manual") {
    return { scheduleType };
  } else if (scheduleType === "basic") {
    return { scheduleType, scheduleData: { basicSchedule: schedule as ConnectionScheduleDataBasicSchedule } };
  }
  return { scheduleType, scheduleData: { cron: schedule as ConnectionScheduleDataCron } };
}
