import { StatusIcon } from "components/ui/StatusIcon";

import { JobWithAttempts } from "area/connection/types/jobs";
import { isJobPartialSuccess, didJobSucceed, getJobStatus } from "area/connection/utils/jobs";
import { JobStatus } from "core/api/types/AirbyteClient";

interface JobStatusIconProps {
  job: JobWithAttempts;
}

export const JobStatusIcon: React.FC<JobStatusIconProps> = ({ job }) => {
  const didSucceed = didJobSucceed(job);
  const jobStatus = getJobStatus(job);
  const jobIsPartialSuccess = isJobPartialSuccess(job.attempts);

  if (jobIsPartialSuccess) {
    return <StatusIcon status="warning" />;
  } else if (!didSucceed) {
    return <StatusIcon status="error" />;
  } else if (jobStatus === JobStatus.cancelled) {
    return <StatusIcon status="cancelled" />;
  } else if (jobStatus === JobStatus.running || jobStatus === JobStatus.incomplete) {
    return <StatusIcon status="loading" />;
  } else if (jobStatus === JobStatus.succeeded) {
    return <StatusIcon status="success" />;
  }
  return null;
};
