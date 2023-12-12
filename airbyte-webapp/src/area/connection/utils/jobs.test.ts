import { mockAttempt } from "test-utils/mock-data/mockAttempt";

import { AttemptRead, JobStatus } from "core/api/types/AirbyteClient";

import { isJobPartialSuccess } from "./jobs";

describe(`${isJobPartialSuccess.name}`, () => {
  it("should return false if attempts is undefined", () => {
    expect(isJobPartialSuccess(undefined)).toBe(false);
  });

  it("should return true if at least one attempt is a partial success", () => {
    const attempts: AttemptRead[] = [
      {
        ...mockAttempt,
        status: JobStatus.failed,
        failureSummary: {
          partialSuccess: true,
          failures: [],
        },
      },
      {
        ...mockAttempt,
        status: JobStatus.failed,
      },
    ];

    expect(isJobPartialSuccess(attempts)).toBe(true);
  });

  it("should return false if no attempts are a partial success", () => {
    const attempts: AttemptRead[] = [
      {
        ...mockAttempt,
        status: JobStatus.failed,
      },
      {
        ...mockAttempt,
        status: JobStatus.failed,
      },
    ];

    expect(isJobPartialSuccess(attempts)).toBe(false);
  });
});
