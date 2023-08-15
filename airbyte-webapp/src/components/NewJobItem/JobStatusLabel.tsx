import { FormattedMessage } from "react-intl";

import { JobWithAttempts } from "components/JobItem/types";
import { getJobAttempts, getJobStatus } from "components/JobItem/utils";
import { Text } from "components/ui/Text";

import { JobStatus } from "core/request/AirbyteClient";

import { isPartialSuccess } from "./isPartialSuccess";

interface JobStatusLabelProps {
  jobWithAttempts: JobWithAttempts;
}

export const JobStatusLabel: React.FC<JobStatusLabelProps> = ({ jobWithAttempts }) => {
  const attempts = getJobAttempts(jobWithAttempts);
  const jobStatus = getJobStatus(jobWithAttempts);
  const jobIsPartialSuccess = isPartialSuccess(attempts);
  const streamsToReset = "job" in jobWithAttempts ? jobWithAttempts.job.resetConfig?.streamsToReset : undefined;
  const jobConfigType = jobWithAttempts.job.configType;

  let status = "";
  if (jobIsPartialSuccess) {
    status = "partialSuccess";
  } else if (jobStatus === JobStatus.failed) {
    status = "failed";
  } else if (jobStatus === JobStatus.cancelled) {
    status = "cancelled";
  } else if (jobStatus === JobStatus.running || jobStatus === JobStatus.incomplete) {
    status = "running";
  } else if (jobStatus === JobStatus.succeeded) {
    status = "succeeded";
  } else {
    return <FormattedMessage id="jobs.jobStatus.unknown" />;
  }
  return (
    <Text>
      <FormattedMessage
        values={{ count: streamsToReset?.length || 0 }}
        id={`jobs.jobStatus.${jobConfigType}.${status}`}
      />
    </Text>
  );
};
