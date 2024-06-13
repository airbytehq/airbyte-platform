import { StatusIcon } from "components/ui/StatusIcon";

import { JobWithAttempts } from "area/connection/types/jobs";
import { getJobStatus } from "area/connection/utils/jobs";
import { JobStatus } from "core/api/types/AirbyteClient";
import { getFailureType } from "core/utils/errorStatusMessage";

interface JobStatusIconProps {
  job: JobWithAttempts;
}

export const JobStatusIcon: React.FC<JobStatusIconProps> = ({ job }) => {
  const jobStatus = getJobStatus(job);
  const lastAttempt = job.attempts?.at(-1); // even if attempts is present it might be empty, which `.at` propagates to `lastAttempt`

  const failureType = lastAttempt?.failureSummary?.failures[0]
    ? getFailureType(lastAttempt?.failureSummary?.failures[0])
    : null;

  if (jobStatus === JobStatus.cancelled) {
    return <StatusIcon status="cancelled" />;
  } else if (failureType === "warning") {
    return <StatusIcon status="warning" />;
  } else if (failureType === "error") {
    return <StatusIcon status="error" />;
  } else if (jobStatus === JobStatus.running || jobStatus === JobStatus.incomplete || jobStatus === JobStatus.pending) {
    return <StatusIcon status="loading" />;
  } else if (jobStatus === JobStatus.succeeded) {
    return <StatusIcon status="success" />;
  }
  return null;
};
