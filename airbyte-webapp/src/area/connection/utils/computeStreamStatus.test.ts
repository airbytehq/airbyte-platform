import dayjs from "dayjs";

import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { mockAirbyteStream } from "test-utils/mock-data/mockAirbyteStream";
import { mockStreamStatusRead } from "test-utils/mock-data/mockStreamStatusRead";

import {
  AirbyteStream,
  ConnectionSyncResultRead,
  JobConfigType,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRead,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";

import { computeStreamStatus, getStreamKey } from "./computeStreamStatus";

const now = dayjs();

const oneYearAgo = now.subtract(1, "year").valueOf();
const oneHourAgo = now.subtract(1, "hour").valueOf();
const threeHoursAgo = now.subtract(3, "hour").valueOf();
const fiveHoursAgo = now.subtract(5, "hour").valueOf();
const anHourFromNow = now.add(1, "hour").valueOf();

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
  it.each`
    description                                                                                      | statuses                                                                                                                                                                                                                                                                      | recordsExtracted | hasBreakingSchemaChange | isSyncing | runningJobConfigType              | expectedStatus                        | expectedIsRunning | expectedLastSuccessfulSync                                                                           | expectedQuotaReset
    ${'returns "Synced" for successful, unscheduled sync'}                                           | ${[buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE })]}                                                                                                                                                                                                       | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Synced}            | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE })}                                | ${undefined}
    ${'returns "Synced" for successful sync, long ago'}                                              | ${[buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneYearAgo })]}                                                                                                                                                                           | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Synced}            | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneYearAgo })}    | ${undefined}
    ${'returns "Synced" with successful sync followed by a cancel'}                                  | ${[buildStreamStatusRead({ runState: StreamStatusRunState.INCOMPLETE, incompleteRunCause: StreamStatusIncompleteRunCause.CANCELED, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo })]}            | ${1}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Synced}            | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo })}    | ${undefined}
    ${'returns "Failed" for breaking schema change after a successful sync'}                         | ${[buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo })]}                                                                                                                                                                           | ${1}             | ${true}                 | ${false}  | ${undefined}                      | ${StreamStatusType.Failed}            | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: oneHourAgo })}    | ${undefined}
    ${'returns "Incomplete" for  a sync that extracted no records'}                                  | ${[buildStreamStatusRead({ runState: StreamStatusRunState.INCOMPLETE, incompleteRunCause: StreamStatusIncompleteRunCause.FAILED, transitionedAt: oneHourAgo })]}                                                                                                              | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Incomplete}        | ${false}          | ${undefined}                                                                                         | ${undefined}
    ${'returns "Incomplete" for failed sync that extracted records'}                                 | ${[buildStreamStatusRead({ runState: StreamStatusRunState.INCOMPLETE, incompleteRunCause: StreamStatusIncompleteRunCause.FAILED, transitionedAt: fiveHoursAgo })]}                                                                                                            | ${1}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Incomplete}        | ${false}          | ${undefined}                                                                                         | ${undefined}
    ${'returns "Incomplete" when the most recent sync failed after a successful sync'}               | ${[buildStreamStatusRead({ runState: StreamStatusRunState.INCOMPLETE, incompleteRunCause: StreamStatusIncompleteRunCause.FAILED, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: threeHoursAgo })]}           | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Incomplete}        | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: threeHoursAgo })} | ${undefined}
    ${'returns "Queued for next sync" when the most recent run state is pending'}                    | ${[buildStreamStatusRead({ runState: StreamStatusRunState.PENDING })]}                                                                                                                                                                                                        | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.QueuedForNextSync} | ${false}          | ${undefined}                                                                                         | ${undefined}
    ${'returns "Queued for next sync" when there are no previous statuses'}                          | ${[]}                                                                                                                                                                                                                                                                         | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.QueuedForNextSync} | ${false}          | ${undefined}                                                                                         | ${undefined}
    ${'returns "Queued for next sync" when the last job was a clear'}                                | ${[buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, jobType: StreamStatusJobType.RESET, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, jobType: StreamStatusJobType.SYNC, transitionedAt: threeHoursAgo })]} | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.QueuedForNextSync} | ${false}          | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: threeHoursAgo })} | ${undefined}
    ${'returns "Queued" for a currently running sync with history'}                                  | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })]}                                                                          | ${0}             | ${false}                | ${false}  | ${undefined}                      | ${StreamStatusType.Queued}            | ${true}           | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })}  | ${undefined}
    ${'returns "Queued" for only a currently running sync with no history'}                          | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo })]}                                                                                                                                                                            | ${0}             | ${false}                | ${true}   | ${JobConfigType.sync}             | ${StreamStatusType.Queued}            | ${true}           | ${undefined}                                                                                         | ${undefined}
    ${'returns "Queued" with a currently running refresh job'}                                       | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })]}                                                                          | ${0}             | ${false}                | ${true}   | ${JobConfigType.refresh}          | ${StreamStatusType.Queued}            | ${true}           | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })}  | ${undefined}
    ${'returns "Syncing" if records were extracted with only a currently running sync (no history)'} | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo })]}                                                                                                                                                                            | ${1}             | ${false}                | ${true}   | ${JobConfigType.sync}             | ${StreamStatusType.Syncing}           | ${true}           | ${undefined}                                                                                         | ${undefined}
    ${'returns "Syncing" if records were extracted  with a currently running sync'}                  | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })]}                                                                          | ${1}             | ${false}                | ${true}   | ${JobConfigType.sync}             | ${StreamStatusType.Syncing}           | ${true}           | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })}  | ${undefined}
    ${'returns "RateLimited" if the most recent status is RATE_LIMITED'}                             | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RATE_LIMITED, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.INCOMPLETE, transitionedAt: threeHoursAgo })]}                                                                  | ${0}             | ${false}                | ${true}   | ${JobConfigType.sync}             | ${StreamStatusType.RateLimited}       | ${true}           | ${undefined}                                                                                         | ${undefined}
    ${'returns "RateLimited" with a quotaReset if the most recent status is RATE_LIMITED'}           | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RATE_LIMITED, transitionedAt: oneHourAgo, metadata: { quotaReset: anHourFromNow } })]}                                                                                                                              | ${5}             | ${false}                | ${true}   | ${JobConfigType.sync}             | ${StreamStatusType.RateLimited}       | ${true}           | ${undefined}                                                                                         | ${anHourFromNow}
    ${'returns "Clearing" when job type is reset_connection'}                                        | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo })]}                                                                                                                                                                            | ${1}             | ${false}                | ${true}   | ${JobConfigType.reset_connection} | ${StreamStatusType.Clearing}          | ${true}           | ${undefined}                                                                                         | ${undefined}
    ${'returns "Clearing" when job type is clear'}                                                   | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })]}                                                                          | ${1}             | ${false}                | ${true}   | ${JobConfigType.clear}            | ${StreamStatusType.Clearing}          | ${true}           | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })}  | ${undefined}
    ${'returns "Refreshing" with records extracted during a currently running refresh'}              | ${[buildStreamStatusRead({ runState: StreamStatusRunState.RUNNING, transitionedAt: oneHourAgo }), buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })]}                                                                          | ${1}             | ${false}                | ${true}   | ${JobConfigType.refresh}          | ${StreamStatusType.Refreshing}        | ${true}           | ${buildStreamStatusRead({ runState: StreamStatusRunState.COMPLETE, transitionedAt: fiveHoursAgo })}  | ${undefined}
  `(
    "$description",
    ({
      statuses,
      recordsExtracted,
      hasBreakingSchemaChange,
      isSyncing,
      runningJobConfigType,
      expectedStatus,
      expectedIsRunning,
      expectedLastSuccessfulSync,
      expectedQuotaReset,
    }) => {
      const result = computeStreamStatus({
        statuses,
        recordsExtracted,
        hasBreakingSchemaChange,
        isSyncing,
        runningJobConfigType,
      });

      expect(result).toEqual({
        status: expectedStatus,
        isRunning: expectedIsRunning,
        lastSuccessfulSync: expectedLastSuccessfulSync,
        quotaReset: expectedQuotaReset,
      });
    }
  );
});

function buildStreamStatusRead({
  jobType = StreamStatusJobType.SYNC,
  runState = StreamStatusRunState.COMPLETE,
  incompleteRunCause,
  transitionedAt = mockStreamStatusRead.transitionedAt,
  jobId = mockStreamStatusRead.jobId,
  metadata = undefined,
}: {
  jobType?: StreamStatusRead["jobType"];
  runState?: StreamStatusRead["runState"];
  incompleteRunCause?: StreamStatusRead["incompleteRunCause"];
  transitionedAt?: StreamStatusRead["transitionedAt"];
  jobId?: StreamStatusRead["jobId"];
  metadata?: StreamStatusRead["metadata"];
}): StreamStatusRead {
  return { ...mockStreamStatusRead, jobType, runState, incompleteRunCause, transitionedAt, jobId, metadata };
}
