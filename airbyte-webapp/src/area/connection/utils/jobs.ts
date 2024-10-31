import { JobRead, LogRead, SynchronousJobRead } from "core/api/types/AirbyteClient";

import { JobWithAttempts } from "../types/jobs";

export const isClearJob = (job: SynchronousJobRead | JobWithAttempts | JobRead): boolean =>
  "configType" in job
    ? job.configType === "clear" || job.configType === "reset_connection"
    : job.job.configType === "clear" || job.job.configType === "reset_connection";

type SyncrhonousJobReadWithFormattedLogs = SynchronousJobRead & { logType: "formatted"; logs: LogRead };

export function jobHasFormattedLogs(job: SynchronousJobRead): job is SyncrhonousJobReadWithFormattedLogs {
  return job.logType === "formatted";
}
