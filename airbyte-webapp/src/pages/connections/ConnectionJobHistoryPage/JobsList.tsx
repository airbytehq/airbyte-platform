import React, { useMemo } from "react";

import { JobItem } from "components/JobItem";
import { JobWithAttempts } from "components/JobItem/types";
import { NewJobItem } from "components/NewJobItem";

import { JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

interface JobsListProps {
  jobs: JobWithAttemptsRead[];
}

const JobsList: React.FC<JobsListProps> = ({ jobs }) => {
  const searchableJobLogsEnabled = useExperiment("connection.searchableJobLogs", true);

  const sortJobReads: JobWithAttempts[] = useMemo(
    () =>
      jobs
        .filter((job): job is JobWithAttempts => !!job.job && !!job.attempts)
        .sort((a, b) => (a.job.createdAt > b.job.createdAt ? -1 : 1)),
    [jobs]
  );

  return (
    <div>
      {searchableJobLogsEnabled &&
        sortJobReads.map((jobWithAttempts) => (
          <NewJobItem key={`newJobItem_${jobWithAttempts.job.id}`} jobWithAttempts={jobWithAttempts} />
        ))}
      {!searchableJobLogsEnabled &&
        sortJobReads.map((jobWithAttempts) => <JobItem key={jobWithAttempts.job.id} job={jobWithAttempts} />)}
    </div>
  );
};

export default JobsList;
