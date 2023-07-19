import { JobWithAttempts } from "components/JobItem/types";
import { didJobSucceed, getJobStatus } from "components/JobItem/utils";
import { StatusIcon } from "components/ui/StatusIcon";

import { AttemptInfoRead, JobStatus, AttemptStatus } from "core/request/AirbyteClient";

import { isPartialSuccess } from "./isPartialSuccess";

interface JobStatusIconProps {
  job: JobWithAttempts;
}

export const JobStatusIcon: React.FC<JobStatusIconProps> = ({ job }) => {
  const didSucceed = didJobSucceed(job);
  const jobStatus = getJobStatus(job);
  const jobIsPartialSuccess = isPartialSuccess(job.attempts);

  if (!jobIsPartialSuccess && !didSucceed) {
    return <StatusIcon status="error" />;
  } else if (jobStatus === JobStatus.cancelled) {
    return <StatusIcon status="cancelled" />;
  } else if (jobStatus === JobStatus.running) {
    return <StatusIcon status="loading" />;
  } else if (jobStatus === JobStatus.succeeded) {
    return <StatusIcon status="success" />;
  } else if (jobIsPartialSuccess) {
    return <StatusIcon status="warning" />;
  }
  return null;
};

interface AttemptStatusIconProps {
  attempt: AttemptInfoRead;
}

export const AttemptStatusIcon: React.FC<AttemptStatusIconProps> = ({ attempt }) => {
  if (attempt.attempt.status === AttemptStatus.failed) {
    return <StatusIcon status="error" />;
  } else if (attempt.attempt.status === AttemptStatus.running) {
    return <StatusIcon status="loading" />;
  } else if (attempt.attempt.status === AttemptStatus.succeeded) {
    return <StatusIcon status="success" />;
  }
  return null;
};
