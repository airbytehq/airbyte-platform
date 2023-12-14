import React, { useMemo } from "react";

import { JobHistoryItem } from "area/connection/components/JobHistoryItem";
import { JobWithAttempts } from "area/connection/types/jobs";
import { JobWithAttemptsRead } from "core/api/types/AirbyteClient";

interface JobsListProps {
  jobs: JobWithAttemptsRead[];
}

const JobsList: React.FC<JobsListProps> = ({ jobs }) => {
  const sortJobReads: JobWithAttempts[] = useMemo(
    () =>
      jobs
        .filter((job): job is JobWithAttempts => !!job.job && !!job.attempts)
        .sort((a, b) => (a.job.createdAt > b.job.createdAt ? -1 : 1)),
    [jobs]
  );

  return (
    <div>
      {sortJobReads.map((jobWithAttempts) => (
        <JobHistoryItem key={jobWithAttempts.job.id} jobWithAttempts={jobWithAttempts} />
      ))}
    </div>
  );
};

export default JobsList;
