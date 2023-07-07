import { AttemptRead, JobStatus } from "core/request/AirbyteClient";

export const isPartialSuccess = (attempts?: AttemptRead[]) => {
  if (!attempts) {
    return false;
  }
  if (attempts.length > 0 && attempts[attempts.length - 1].status === JobStatus.failed) {
    return attempts.some((attempt) => attempt.failureSummary && attempt.failureSummary.partialSuccess);
  }
  return false;
};
