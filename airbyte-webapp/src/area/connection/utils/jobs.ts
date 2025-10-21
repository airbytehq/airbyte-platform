import { JobRead, LogEvents, LogRead, SynchronousJobRead } from "core/api/types/AirbyteClient";

import { JobWithAttempts } from "../types/jobs";

export const isClearJob = (job: SynchronousJobRead | JobWithAttempts | JobRead): boolean =>
  "configType" in job
    ? job.configType === "clear" || job.configType === "reset_connection"
    : job.job.configType === "clear" || job.job.configType === "reset_connection";

type SynchronousJobReadWithFormattedLogs = SynchronousJobRead & { logType: "formatted"; logs: LogRead };
type SynchronousJobReadWithStructuredLogs = SynchronousJobRead & { logType: "structured"; logs: LogEvents };

export function areLogsStructured(logs: LogRead | LogEvents): logs is LogEvents {
  return "logLines" in logs === false && "events" in logs;
}

export function areLogsFormatted(logs: LogRead | LogEvents): logs is LogRead {
  return "logLines" in logs && "events" in logs === false;
}

export function jobHasFormattedLogs(job: SynchronousJobRead): job is SynchronousJobReadWithFormattedLogs {
  return job.logType === "formatted";
}

export function jobHasStructuredLogs(job: SynchronousJobRead): job is SynchronousJobReadWithStructuredLogs {
  return job.logType === "structured";
}
