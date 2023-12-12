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

const oneYearAgo = dayjs().subtract(1, "year").valueOf();
const oneHourAgo = dayjs().subtract(1, "hour").valueOf(); // within the 2-hour window of `basicScheduleData`
const threeHoursAgo = dayjs().subtract(3, "hour").valueOf(); // 1 step outside the 2-hour window of `basicScheduleData`
const fiveHoursAgo = dayjs().subtract(5, "hour").valueOf(); // 2 steps outside the 2-hour window of `basicScheduleData`

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
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({ status: undefined, isRunning: false, lastSuccessfulSync: undefined });
    });

    it('returns "undefined" with only a currently running sync (not enough history)', () => {
      const status = buildStreamStatusRead({
        runState: StreamStatusRunState.RUNNING,
        transitionedAt: oneHourAgo,
      });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: undefined,
        isRunning: true,
        lastSuccessfulSync: undefined,
      });
    });
  });

  describe("pending", () => {
    it('returns "Pending" when the most recent run state is pending', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.PENDING });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });

    it('returns "Pending" when the most recent job is reset and is complete', () => {
      const status = buildStreamStatusRead({
        jobType: StreamStatusJobType.RESET,
        runState: StreamStatusRunState.COMPLETE,
      });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });

    it('returns "Pending" when the most recent job is reset even if it is incomplete', () => {
      const status = buildStreamStatusRead({
        jobType: StreamStatusJobType.RESET,
        runState: StreamStatusRunState.INCOMPLETE,
      });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        lastSuccessfulSync: undefined,
      });
    });
  });

  describe("on time", () => {
    it('returns "OnTime" when the most recent sync was successful, unscheduled', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: undefined,
        scheduleData: undefined,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "OnTime" when the most recent sync (long ago) was successful, cron', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneYearAgo });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.cron,
        scheduleData: cronScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "OnTime" when the most recent sync was successful, basic', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
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
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTime,
        isRunning: false,
        lastSuccessfulSync: successStatus,
      });
    });
  });

  describe("late", () => {
    it('returns "OnTrack" when the most recent sync was successful but one late', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: threeHoursAgo });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.OnTrack,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });

    it('returns "Late" when the most recent sync was successful but two late', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: false,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.Late,
        isRunning: false,
        lastSuccessfulSync: status,
      });
    });
  });

  describe("action required", () => {
    it('returns "ActionRequired" when there is a breaking schema change (otherwise ontime)', () => {
      const status = buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo });
      const result = computeStreamStatus({
        statuses: [status],
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.ActionRequired,
        isRunning: false,
        lastSuccessfulSync: status,
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
        scheduleType: ConnectionScheduleType.basic,
        scheduleData: basicScheduleData,
        hasBreakingSchemaChange: true,
        lateMultiplier: 2,
        errorMultiplier: 2,
      });
      expect(result).toEqual({
        status: ConnectionStatusIndicatorStatus.ActionRequired,
        isRunning: false,
        lastSuccessfulSync: undefined,
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
