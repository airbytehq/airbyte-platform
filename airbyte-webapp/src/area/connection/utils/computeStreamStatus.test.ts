import dayjs from "dayjs";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { mockAirbyteStream } from "test-utils/mock-data/mockAirbyteStream";
import { mockStreamStatusRead } from "test-utils/mock-data/mockStreamStatusRead";

import {
  AirbyteStream,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionSyncResultRead,
  JobConfigType,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRead,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";

import { computeStreamStatus, getStreamKey } from "./computeStreamStatus";

const cronScheduleData: ConnectionScheduleData = { cron: { cronExpression: "* * * * *", cronTimeZone: "UTC" } };
const basicScheduleData: ConnectionScheduleData = { basicSchedule: { timeUnit: "hours", units: 2 } };

const now = dayjs();

const oneYearAgo = now.subtract(1, "year").valueOf();
const oneHourAgo = now.subtract(1, "hour").valueOf(); // within the 2-hour window of `basicScheduleData`
const threeHoursAgo = now.subtract(3, "hour").valueOf(); // 1 step outside the 2-hour window of `basicScheduleData`
const fiveHoursAgo = now.subtract(5, "hour").valueOf(); // 2 steps outside the 2-hour window of `basicScheduleData`

describe("getStreamKey", () => {
  describe("when streamStatus is a StreamStatusRead", () => {
    it.each`
      name     | namespace    | expected
      ${"foo"} | ${"bar"}     | ${"foo-bar"}
      ${"foo"} | ${""}        | ${"foo-"}
      ${"foo"} | ${undefined} | ${"foo-"}
    `("$name, $namespace", ({ name, namespace, expected }) =>
      expect(
        getStreamKey({ ...mockStreamStatusRead, streamName: name, streamNamespace: namespace } as StreamStatusRead)
      ).toBe(expected)
    );
  });

  describe("when streamStatus is an AirbyteStream", () => {
    it.each`
      name     | namespace    | expected
      ${"foo"} | ${"bar"}     | ${"foo-bar"}
      ${"foo"} | ${""}        | ${"foo-"}
      ${"foo"} | ${undefined} | ${"foo-"}
    `("$name, $namespace", ({ name, namespace, expected }) =>
      expect(getStreamKey({ ...mockAirbyteStream, name, namespace } as AirbyteStream)).toBe(expected)
    );
  });

  describe("when streamStatus is a ConnectionSyncResultRead", () => {
    it.each`
      name     | namespace    | expected
      ${"foo"} | ${"bar"}     | ${"foo-bar"}
      ${"foo"} | ${""}        | ${"foo-"}
      ${"foo"} | ${undefined} | ${"foo-"}
    `("$name, $namespace", ({ name, namespace, expected }) =>
      expect(getStreamKey({ streamName: name, streamNamespace: namespace } as ConnectionSyncResultRead)).toBe(expected)
    );
  });
});

describe("computeStreamStatus", () => {
  describe("on time", () => {
    it('returns "OnTime" when the most recent sync was successful, unscheduled', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Synced,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "OnTime" when the most recent sync (long ago) was successful, cron', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneYearAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        scheduleType: ConnectionScheduleType.cron,
        scheduleData: cronScheduleData,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Synced,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "OnTime" when the most recent sync was successful, basic', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Synced,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "OnTime" with a successful recent sync followed by a cancel', () => {
      const successStatus = buildStreamStatusRead({
        runState: StreamStatusRunState.COMPLETE,
        transitionedAt: oneHourAgo,
      });
      const cancelStatus = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED,
        transitionedAt: oneHourAgo,
      });
      const result = computeStreamStatus({
        statuses: [cancelStatus, successStatus],
        recordsExtracted: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Synced,
        isRunning: false,
        lastSuccessfulSync: successStatus,
      });
    });
  });

  describe("failed", () => {
    it('returns "Failed" when there is a breaking schema change (otherwise ontime)', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Failed,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "Failed" when there is a breaking schema change (otherwise error)', () => {
      const status = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
        transitionedAt: oneHourAgo,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Failed,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });
  });

  describe("error", () => {
    it('returns "Error" when there is one INCOMPLETE stream status two steps outside the 2-hour window', () => {
      const status = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
        transitionedAt: fiveHoursAgo,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Incomplete,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });

    it('returns "Error" when there is one INCOMPLETE stream status and the connection frequency is manual', () => {
      const status = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
        transitionedAt: oneHourAgo,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        scheduleType: ConnectionScheduleType.manual,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Incomplete,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });

    it('returns "Error" as the most recent sync failed, even though there successful sync within the 2x window, as the scheduling is not basic', () => {
      const failedStatus = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
        transitionedAt: oneHourAgo,
      });
      const successStatus = buildStreamStatusRead({
        runState: StreamStatusRunState.COMPLETE,
        transitionedAt: threeHoursAgo,
      });
      const result = computeStreamStatus({
        statuses: [failedStatus, successStatus],
        recordsExtracted: 0,
        scheduleType: ConnectionScheduleType.manual,
        hasBreakingSchemaChange: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Incomplete,
        isRunning: false,
        lastSuccessfulSync: successStatus,
      });
    });
  });

  describe("with sync progress shown", () => {
    describe("queued for next sync", () => {
      it('returns "Queued for next sync" when the most recent run state is pending', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.PENDING });
        const result = computeStreamStatus({
          statuses: [status],
          recordsExtracted: 0,
          scheduleType: undefined,
          scheduleData: undefined,
          hasBreakingSchemaChange: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.QueuedForNextSync,
          isRunning: false,
          lastSuccessfulSync: undefined,
        });
      });

      it('returns "queued for next sync" when there are no statuses', () => {
        const result = computeStreamStatus({
          statuses: [],
          recordsExtracted: 0,
          scheduleType: undefined,
          scheduleData: undefined,
          hasBreakingSchemaChange: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.QueuedForNextSync,
          isRunning: false,
          lastSuccessfulSync: undefined,
        });
      });
      it('returns "queued for next sync" when the last job was a reset', () => {
        const resetStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          jobType: StreamStatusJobType.RESET,
          transitionedAt: oneHourAgo,
        });
        const syncStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          jobType: StreamStatusJobType.RESET,
          transitionedAt: threeHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [resetStatus, syncStatus],
          recordsExtracted: 0,
          scheduleType: undefined,
          scheduleData: undefined,
          hasBreakingSchemaChange: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.QueuedForNextSync,
          isRunning: false,
          lastSuccessfulSync: undefined,
        });
      });
    });
    describe("queued", () => {
      it("returns 'Queued' with a currently running sync that is behind schedule", () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Queued,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
      it('returns "queued" with only a currently running sync (no history)', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus],
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
          recordsExtracted: 0,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Queued,
          isRunning: true,
          lastSuccessfulSync: undefined,
        });
      });
      it('returns "queued" with a currently running sync', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Queued,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
      it('returns "queued with a currently running refresh', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.refresh,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Queued,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
    });
    describe("syncing", () => {
      it('returns "syncing" if records were extracted - with only a currently running sync (no history)', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus],
          recordsExtracted: 1,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: undefined,
        });
      });
      it('returns "syncing" if records were extracted - with a currently running sync', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 1,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
    });
    describe("rateLimited", () => {
      it('returns "rateLimited" if the most recent status is RATE_LIMITED', () => {
        const rateLimitedStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RATE_LIMITED,
          transitionedAt: oneHourAgo,
        });
        const incompleteStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.INCOMPLETE,
          transitionedAt: threeHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [rateLimitedStatus, incompleteStatus],
          recordsExtracted: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.RateLimited,
          isRunning: true,
          lastSuccessfulSync: undefined,
          quotaReset: undefined,
        });
      });
      it('returns "rateLimited" with a quotaReset if the most recent status is RATE_LIMITED', () => {
        const rateLimitedStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RATE_LIMITED,
          transitionedAt: oneHourAgo,
        });
        const quotaReset = Date.now() + 60000;
        rateLimitedStatus.metadata = { quotaReset };

        const result = computeStreamStatus({
          statuses: [rateLimitedStatus],
          recordsExtracted: 5,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.sync,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.RateLimited,
          isRunning: true,
          lastSuccessfulSync: undefined,
          quotaReset,
        });
      });
    });
    describe("clearing", () => {
      it('returns "clearing" when job type is reset_connection', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus],
          recordsExtracted: 1,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.reset_connection,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Clearing,
          isRunning: true,
          lastSuccessfulSync: undefined,
        });
      });
      it('returns "clearing" when job type is clear', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 1,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.clear,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Clearing,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
    });
    describe("refreshing", () => {
      it('returns "refreshing" with a currently running refresh', () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 1,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          isSyncing: true,
          runningJobConfigType: JobConfigType.refresh,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Refreshing,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
        });
      });
    });
  });
});

function buildStreamStatusRead({
  jobType = StreamStatusJobType.SYNC,
  runState = StreamStatusRunState.COMPLETE,
  incompleteRunCause,
  transitionedAt = mockStreamStatusRead.transitionedAt,
  jobId = mockStreamStatusRead.jobId,
}: {
  jobType?: StreamStatusRead["jobType"];
  runState?: StreamStatusRead["runState"];
  incompleteRunCause?: StreamStatusRead["incompleteRunCause"];
  transitionedAt?: StreamStatusRead["transitionedAt"];
  jobId?: StreamStatusRead["jobId"];
}): StreamStatusRead {
  return { ...mockStreamStatusRead, jobType, runState, incompleteRunCause, transitionedAt, jobId };
}
