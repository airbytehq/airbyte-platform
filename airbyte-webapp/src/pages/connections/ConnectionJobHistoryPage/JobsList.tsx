import React, { useMemo } from "react";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { JobItem } from "components/JobItem";
import { JobsWithJobs } from "components/JobItem/types";

import { JobStatus, JobWithAttemptsRead } from "core/request/AirbyteClient";

interface JobsListProps {
  jobs: JobWithAttemptsRead[];
}

const JobsList: React.FC<JobsListProps> = ({ jobs }) => {
  const { activeJob } = useConnectionSyncContext();
  const sortJobs: JobsWithJobs[] = useMemo(
    () =>
      jobs.filter((job): job is JobsWithJobs => !!job.job).sort((a, b) => (a.job.createdAt > b.job.createdAt ? -1 : 1)),
    [jobs]
  );

  return (
    <div>
      {activeJob && activeJob.id !== sortJobs?.[0]?.job?.id && (
        <JobItem key={`${activeJob.id}activeJob`} job={{ job: { ...activeJob, status: JobStatus.running } }} />
      )}
      {sortJobs.map((job) => (
        <JobItem key={job.job.id} job={job} />
      ))}
    </div>
  );
};

export default JobsList;
