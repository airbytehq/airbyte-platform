import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { JobWithAttempts } from "area/connection/types/jobs";
import { isJobPartialSuccess, getJobAttempts, getJobStatus } from "area/connection/utils/jobs";
import { JobStatus } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

interface JobStatusLabelProps {
  jobWithAttempts: JobWithAttempts;
}

export const JobStatusLabel: React.FC<JobStatusLabelProps> = ({ jobWithAttempts }) => {
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);

  const attempts = getJobAttempts(jobWithAttempts);
  const jobStatus = getJobStatus(jobWithAttempts);
  const jobIsPartialSuccess = isJobPartialSuccess(attempts);
  const streamsToReset = "job" in jobWithAttempts ? jobWithAttempts.job.resetConfig?.streamsToReset : undefined;
  const jobConfigType =
    sayClearInsteadOfReset && jobWithAttempts.job.configType === "reset_connection"
      ? "clear_data"
      : jobWithAttempts.job.configType;

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
