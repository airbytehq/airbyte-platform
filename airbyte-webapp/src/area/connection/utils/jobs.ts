import { JobRead, SynchronousJobRead } from "core/api/types/AirbyteClient";

import { JobWithAttempts } from "../types/jobs";

export const isClearJob = (job: SynchronousJobRead | JobWithAttempts | JobRead): boolean =>
  "configType" in job
    ? job.configType === "clear" || job.configType === "reset_connection"
    : job.job.configType === "clear" || job.job.configType === "reset_connection";
