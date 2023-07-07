import { AttemptRead, JobStatus } from "core/request/AirbyteClient";

export const mockAttempt: AttemptRead = {
  id: 1,
  status: JobStatus.failed,
  createdAt: 0,
  updatedAt: 0,
  endedAt: 0,
};
