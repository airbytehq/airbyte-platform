import { JobWithAttemptsRead } from "core/request/AirbyteClient";

// JobWithAttemptsRead has an optional job property, but we really want it to be required
export interface JobWithAttempts extends JobWithAttemptsRead {
  job: NonNullable<JobWithAttemptsRead["job"]>;
  attempts: NonNullable<JobWithAttemptsRead["attempts"]>;
}
