import dayjs from "dayjs";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { mockAirbyteStream } from "test-utils/mock-data/mockAirbyteStream";
import { mockStreamStatusRead } from "test-utils/mock-data/mockStreamStatusRead";

import {
  AirbyteStream,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionSyncResultRead,
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
  describe("undefined", () => {
    it('returns "undefined" when there are no statuses', () => {
      const result = computeStreamStatus({
        statuses: [],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: undefined,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });
  });

  describe("pending", () => {
    it('returns "Pending" when the most recent run state is pending', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.PENDING });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "Pending" when the most recent job is reset and is complete', () => {
      const status = buildStreamStatusRead({
        jobType: StreamStatusJobType.RESET,
        runState: StreamStatusRunState.COMPLETE,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "Pending" when the most recent job is reset even if it is incomplete', () => {
      const status = buildStreamStatusRead({
        jobType: StreamStatusJobType.RESET,
        runState: StreamStatusRunState.INCOMPLETE,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });
  });

  describe("on time", () => {
    it('returns "OnTime" when the most recent sync was successful, unscheduled', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "OnTime" when the most recent sync (long ago) was successful, cron', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneYearAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 0,
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.cron,
        scheduleData: cronScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "OnTime" when the most recent sync was successful, basic', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        recordsLoaded: 1,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 1,
        recordsLoaded: 1,
        streamSyncStartedAt: undefined,
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
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: successStatus,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });
  });

  describe("late", () => {
    it('returns "OnTrack" when the most recent sync was successful but one late', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: threeHoursAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTrack,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "OnTrack" when the most recent sync failed but had a successful sync within the 2x window', () => {
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
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTrack,
        isRunning: false,
        lastSuccessfulSync: successStatus,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "Late" when the most recent sync was successful but two late', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Late,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });
  });

  describe("action required", () => {
    it('returns "ActionRequired" when there is a breaking schema change (otherwise ontime)', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        lateMultiplier: 2,
        showSyncProgress: false,
        errorMultiplier: 2,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.ActionRequired,
        isRunning: false,
        lastSuccessfulSync: status,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });

    it('returns "ActionRequired" when there is a breaking schema change (otherwise error)', () => {
      const status = buildStreamStatusRead({
        runState: StreamStatusRunState.INCOMPLETE,
        incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
        transitionedAt: oneHourAgo,
      });
      const result = computeStreamStatus({
        statuses: [status],
        recordsExtracted: 1,
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.ActionRequired,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
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
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        showSyncProgress: false,
        errorMultiplier: 2,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Error,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 1,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
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
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.manual,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        showSyncProgress: false,
        errorMultiplier: 2,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Error,
        isRunning: false,
        lastSuccessfulSync: undefined,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
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
        recordsLoaded: 0,
        scheduleType: ConnectionScheduleType.manual,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
        showSyncProgress: false,
        isSyncing: false,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Error,
        isRunning: false,
        lastSuccessfulSync: successStatus,
        recordsExtracted: 0,
        recordsLoaded: 0,
        streamSyncStartedAt: undefined,
      });
    });
  });

  describe("without sync progress shown", () => {
    describe("queued", () => {
      it("returns undefined with only a currently running sync (no history)", () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });
        const cancelStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.INCOMPLETE,
          incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus, cancelStatus],
          recordsExtracted: 0,
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: undefined,
          isRunning: true,
          lastSuccessfulSync: undefined,
          recordsExtracted: 0,
          recordsLoaded: 0,
          streamSyncStartedAt: undefined,
        });
      });
      it("returns late with a currently running sync that is behind schedule", () => {
        const status = buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo });
        const prevStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.COMPLETE,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [status, prevStatus],
          recordsExtracted: 0,
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Late,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
          recordsExtracted: 0,
          recordsLoaded: 0,
          streamSyncStartedAt: undefined,
        });
      });
    });
    describe("syncing", () => {
      it('returns "undefined" if records were extracted - with only a currently running sync (no history)', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });
        const cancelStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.INCOMPLETE,
          incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus, cancelStatus],
          recordsExtracted: 1,
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: false,
          isSyncing: false,
        });
        expect(result).toEqual({
          status: undefined,
          isRunning: true,
          lastSuccessfulSync: undefined,
          recordsExtracted: 1,
          recordsLoaded: 0,
          streamSyncStartedAt: undefined,
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
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: true,
          isSyncing: true,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
          recordsExtracted: 1,
          recordsLoaded: 0,
          streamSyncStartedAt: oneHourAgo,
        });
      });
    });
  });
  describe("with sync progress shown", () => {
    describe("queued", () => {
      it('returns "queued" with only a currently running sync (no history)', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });
        const cancelStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.INCOMPLETE,
          incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus, cancelStatus],
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: true,
          isSyncing: true,
          recordsExtracted: 1,
          recordsLoaded: 0,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: undefined,
          recordsExtracted: 1,
          recordsLoaded: 0,
          streamSyncStartedAt: oneHourAgo,
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
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: true,
          isSyncing: true,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Queued,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
          recordsExtracted: 0,
          recordsLoaded: 0,
          streamSyncStartedAt: oneHourAgo,
        });
      });
    });
    describe("syncing", () => {
      it('returns "syncing" if records were extracted - with only a currently running sync (no history)', () => {
        const runningStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.RUNNING,
          transitionedAt: oneHourAgo,
        });
        const cancelStatus = buildStreamStatusRead({
          runState: StreamStatusRunState.INCOMPLETE,
          incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED,
          transitionedAt: fiveHoursAgo,
        });

        const result = computeStreamStatus({
          statuses: [runningStatus, cancelStatus],
          recordsExtracted: 1,
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: true,
          isSyncing: true,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: undefined,
          recordsExtracted: 1,
          recordsLoaded: 0,
          streamSyncStartedAt: oneHourAgo,
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
          recordsLoaded: 0,
          scheduleType: ConnectionScheduleType.basic,
          scheduleData: basicScheduleData,
          hasBreakingSchemaChange: false,
          lateMultiplier: 2,
          errorMultiplier: 2,
          showSyncProgress: true,
          isSyncing: true,
        });
        expect(result).toEqual({
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning: true,
          lastSuccessfulSync: prevStatus,
          recordsExtracted: 1,
          recordsLoaded: 0,
          streamSyncStartedAt: oneHourAgo,
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
}: {
  jobType?: StreamStatusRead["jobType"];
  runState?: StreamStatusRead["runState"];
  incompleteRunCause?: StreamStatusRead["incompleteRunCause"];
  transitionedAt?: StreamStatusRead["transitionedAt"];
}): StreamStatusRead {
  return { ...mockStreamStatusRead, jobType, runState, incompleteRunCause, transitionedAt };
}
