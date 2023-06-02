import { useMutation, useQuery } from "@tanstack/react-query";

import { SCOPE_WORKSPACE } from "services/Scope";

import { cancelJob, getJobDebugInfo, listJobsFor } from "../generated/AirbyteClient";
import { JobListRequestBody, JobReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListJobs = (listParams: JobListRequestBody, keepPreviousData = true) => {
  const requestOptions = useRequestOptions();
  const result = useQuery(
    [SCOPE_WORKSPACE, "jobs", "list", listParams.configId, listParams.includingJobId, listParams.pagination],
    () => listJobsFor(listParams, requestOptions),
    {
      // 2.5 second refresh
      refetchInterval: 2500,
      keepPreviousData,
      suspense: true,
    }
  );
  // cast to JobReadList because (suspense: true) means we will never get undefined
  const jobReadList = result.data as JobReadList;
  return { jobs: jobReadList.jobs, totalJobCount: jobReadList.totalJobCount, isPreviousData: result.isPreviousData };
};

export const useGetDebugInfoJob = (id: number, enabled = true, refetchWhileRunning = false) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "jobs", "getDebugInfo", id],
    () => getJobDebugInfo({ id }, requestOptions),
    {
      refetchInterval: !refetchWhileRunning
        ? false
        : (data) => {
            // If refetchWhileRunning was true, we keep refetching debug info (including logs), while the job is still
            // running or hasn't ended too long ago. We need some time after the last attempt has stopped, since logs
            // keep incoming for some time after the job has already been marked as finished.
            const lastAttemptEndTimestamp =
              data?.attempts.length && data.attempts[data.attempts.length - 1].attempt.endedAt;
            // While no attempt ended timestamp exists yet (i.e. the job is still running) or it hasn't ended
            // more than 2 minutes (2 * 60 * 1000ms) ago, keep refetching
            return lastAttemptEndTimestamp && Date.now() - lastAttemptEndTimestamp * 1000 > 2 * 60 * 1000
              ? false
              : 2500;
          },
      enabled,
    }
  );
};

export const useCancelJob = () => {
  const requestOptions = useRequestOptions();
  return useMutation((id: number) => cancelJob({ id }, requestOptions));
};
