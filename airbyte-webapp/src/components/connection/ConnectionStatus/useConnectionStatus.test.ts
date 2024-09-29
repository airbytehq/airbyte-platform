import { UseQueryResult } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";
import dayjs from "dayjs";

import { mockConnection } from "test-utils/mock-data/mockConnection";

import { useListConnectionsStatuses, useGetConnection, useGetConnectionSyncProgress } from "core/api";
import {
  ConnectionScheduleDataBasicSchedule,
  ConnectionScheduleDataCron,
  ConnectionStatus,
  ConnectionStatusRead,
  ConnectionSyncProgressRead,
  FailureType,
  JobConfigType,
  JobStatus,
  SchemaChange,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";

import { useConnectionStatus } from "./useConnectionStatus";
import { ConnectionStatusType } from "../ConnectionStatusIndicator";
import { jobStatusesIndicatingFinishedExecution } from "../ConnectionSync/ConnectionSyncContext";

jest.mock("hooks/connection/useSchemaChanges");
type RequiredSchemaChangesShape = Pick<ReturnType<typeof useSchemaChanges>, "hasBreakingSchemaChange">;
const mockUseSchemaChanges = useSchemaChanges as unknown as jest.Mock<RequiredSchemaChangesShape>;

jest.mock("core/api");
const mockUseGetConnection = useGetConnection as unknown as jest.Mock<WebBackendConnectionRead>;

jest.mock("core/api");
const mockUseListConnectionsStatuses = useListConnectionsStatuses as unknown as jest.Mock<
  Array<Partial<ConnectionStatusRead>>
>;

jest.mock("core/api");
const mockUseGetConnectionSyncProgress = useGetConnectionSyncProgress as unknown as jest.Mock<
  Partial<UseQueryResult<ConnectionSyncProgressRead, unknown>>
>;

interface MockSetup {
  // connection values
  connectionStatus: WebBackendConnectionRead["status"];
  schemaChange: WebBackendConnectionRead["schemaChange"];
  scheduleType?: WebBackendConnectionRead["scheduleType"];
  scheduleData?: WebBackendConnectionRead["scheduleData"];

  // status(es)
  connectionStatuses: Array<Partial<ConnectionStatusRead>>;
}
const resetAndSetupMocks = ({
  connectionStatus,
  schemaChange,
  scheduleType,
  scheduleData,
  connectionStatuses,
}: MockSetup) => {
  if (schemaChange === SchemaChange.breaking && connectionStatus !== ConnectionStatus.inactive) {
    // platform disables a connection when there is a breaking schema change
    throw new Error("A breaking schema change should always result in an inactive connection");
  }

  const hasBreakingSchemaChange = schemaChange === SchemaChange.breaking;

  mockUseGetConnection.mockImplementation(() => ({
    ...mockConnection,
    status: connectionStatus,
    schemaChange,
    scheduleType,
    scheduleData,
  }));

  mockUseListConnectionsStatuses.mockReturnValue(connectionStatuses);

  mockUseSchemaChanges.mockImplementation(() => ({
    hasBreakingSchemaChange,
  }));
  mockUseGetConnectionSyncProgress.mockReturnValue({
    data: { connectionId: mockConnection.connectionId, streams: [], configType: JobConfigType.sync },
  });
};

describe("useConnectionStatus", () => {
  const within1xFrequency = dayjs().subtract(20, "hours").unix();
  const within2xFrequency = dayjs().subtract(28, "hours").unix();
  const outside2xFrequency = dayjs().subtract(50, "hours").unix();

  it.each`
    title                                                                                           | expectedConnectionStatus           | connectionStatus             | schemaChange                 | connectionStatuses                                                      | scheduleType | scheduleData
    ${"most recent sync was successful, no schema changes"}                                         | ${ConnectionStatusType.Synced}     | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.succeeded, within1xFrequency)}  | ${undefined} | ${undefined}
    ${"most recent sync was successful, breaking schema changes"}                                   | ${ConnectionStatusType.Failed}     | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${buildConnectionStatusesRead(JobStatus.succeeded, within1xFrequency)}  | ${undefined} | ${undefined}
    ${"breaking schema changes, sync is within 1x frequency"}                                       | ${ConnectionStatusType.Failed}     | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${buildConnectionStatusesRead(JobStatus.failed, within1xFrequency)}     | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"breaking schema changes, sync is within 2x frequency"}                                       | ${ConnectionStatusType.Failed}     | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${buildConnectionStatusesRead(JobStatus.failed, within2xFrequency)}     | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"breaking schema changes, sync is outside of 2x frequency"}                                   | ${ConnectionStatusType.Failed}     | ${ConnectionStatus.inactive} | ${SchemaChange.breaking}     | ${buildConnectionStatusesRead(JobStatus.failed, outside2xFrequency)}    | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"new connection, not scheduled"}                                                              | ${ConnectionStatusType.Pending}    | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead()}                                        | ${undefined} | ${undefined}
    ${"new connection, scheduled"}                                                                  | ${ConnectionStatusType.Pending}    | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead()}                                        | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"connection status is failed, no previous success"}                                           | ${ConnectionStatusType.Incomplete} | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.failed, undefined)}             | ${undefined} | ${undefined}
    ${"connection status is failed, last previous success was within 2x schedule frequency (cron)"} | ${ConnectionStatusType.Incomplete} | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.failed, within2xFrequency)}     | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"connection status is failed, last previous success was outside 2x schedule frequency"}       | ${ConnectionStatusType.Incomplete} | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.failed, outside2xFrequency)}    | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync was successful, but the next sync hasn't started (outside 2x frequency, cron)"}    | ${ConnectionStatusType.Synced}     | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${buildConnectionStatusesRead(JobStatus.succeeded, outside2xFrequency)} | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"last sync was cancelled, but the next cron-scheduled sync hasn't started"}                   | ${ConnectionStatusType.Synced}     | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${buildConnectionStatusesRead(JobStatus.cancelled, within1xFrequency)}  | ${"cron"}    | ${{ cronExpression: "* * * * *", cronTimeZone: "UTC" }}
    ${"last sync was cancelled, but last successful sync is within 1x frequency"}                   | ${ConnectionStatusType.Synced}     | ${ConnectionStatus.active}   | ${SchemaChange.non_breaking} | ${buildConnectionStatusesRead(JobStatus.cancelled, within1xFrequency)}  | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"last sync has a config_error"}                                                               | ${ConnectionStatusType.Failed}     | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConfigErrorConnectionStatusesRead(within1xFrequency)}            | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"most recent completed job was a successful reset"}                                           | ${ConnectionStatusType.Pending}    | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.succeeded, undefined, true)}    | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
    ${"most recent completed job was a failed reset"}                                               | ${ConnectionStatusType.Pending}    | ${ConnectionStatus.active}   | ${SchemaChange.no_change}    | ${buildConnectionStatusesRead(JobStatus.failed, undefined, true)}       | ${"basic"}   | ${{ units: 24, timeUnit: "hours" }}
  `(
    "$title:" +
      "\n\treturns $expectedConnectionStatus when" +
      "\n\ta connection's status is $connectionStatus" +
      "\n\ta connection's last completed sync status is $connectionSyncStatus" +
      "\n\twith a $scheduleType schedule: $scheduleData" +
      "\n\tand has $schemaChange schema changes",
    ({ expectedConnectionStatus, scheduleType, scheduleData, ...mockConfig }) => {
      resetAndSetupMocks({ ...mockConfig, ...buildScheduleData(scheduleType, scheduleData) });
      const { result } = renderHook(() => useConnectionStatus(mockConnection.connectionId));
      expect(result.current.status).toBe(expectedConnectionStatus);
    }
  );

  it("sets isRunning to true when the current job status is incomplete", () => {
    resetAndSetupMocks({
      connectionStatus: ConnectionStatus.active,
      schemaChange: SchemaChange.no_change,
      connectionStatuses: buildConnectionStatusesRead(JobStatus.incomplete, undefined),
    });
    const { result } = renderHook(() => useConnectionStatus(mockConnection.connectionId));
    expect(result.current.isRunning).toBe(true);
  });
});

type ScheduleData = Pick<WebBackendConnectionRead, "scheduleType" | "scheduleData">;
function buildScheduleData(type: "manual", schedule?: undefined): ScheduleData;
function buildScheduleData(type: "basic", schedule: ConnectionScheduleDataBasicSchedule): ScheduleData;
function buildScheduleData(type: "cron", schedule: ConnectionScheduleDataCron): ScheduleData;
function buildScheduleData(
  scheduleType: WebBackendConnectionRead["scheduleType"],
  schedule: ConnectionScheduleDataBasicSchedule | ConnectionScheduleDataCron | undefined
): ScheduleData {
  if (scheduleType === "manual") {
    return { scheduleType };
  } else if (scheduleType === "basic") {
    return { scheduleType, scheduleData: { basicSchedule: schedule as ConnectionScheduleDataBasicSchedule } };
  }
  return { scheduleType, scheduleData: { cron: schedule as ConnectionScheduleDataCron } };
}

function buildConnectionStatusesRead(
  latestSyncStatus?: JobStatus,
  lastSuccessfulSync?: number,
  lastJobWasReset?: boolean
) {
  return [
    {
      connectionId: mockConnection.connectionId,
      lastSyncJobStatus: latestSyncStatus,
      [lastSuccessfulSync ? "lastSuccessfulSync" : ""]: lastSuccessfulSync,
      isRunning: latestSyncStatus ? !jobStatusesIndicatingFinishedExecution.includes(latestSyncStatus) : false,
      isLastCompletedJobReset: !!lastJobWasReset,
    },
  ] as Array<Partial<ConnectionStatusRead>>;
}

function buildConfigErrorConnectionStatusesRead(lastSuccessfulSync: number | undefined) {
  const jobs = buildConnectionStatusesRead(JobStatus.failed, lastSuccessfulSync);
  jobs[0].failureReason = {
    failureType: FailureType.config_error,
    timestamp: Date.now(),
  };
  return jobs;
}
