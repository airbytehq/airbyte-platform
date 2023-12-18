import { Updater, useInfiniteQuery, useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { jobStatusesIndicatingFinishedExecution } from "components/connection/ConnectionSync/ConnectionSyncContext";

import {
  cancelJob,
  getAttemptForJob,
  getJobDebugInfo,
  getJobInfoWithoutLogs,
  listJobsFor,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { JobListRequestBody, JobReadList } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const jobsKeys = {
  all: (connectionId: string) => [SCOPE_WORKSPACE, connectionId] as const,
  list: (connectionId: string, filters: string | Record<string, string | number | undefined> = {}) =>
    [...jobsKeys.all(connectionId), { filters }] as const,
  useListJobsForConnectionStatus: (connectionId: string) =>
    [...jobsKeys.all(connectionId), "connectionStatus"] as const,
};

export const useListJobs = (requestParams: Omit<JobListRequestBody, "pagination">, pageSize: number) => {
  const requestOptions = useRequestOptions();
  const queryKey = jobsKeys.list(requestParams.configId, {
    includingJobId: requestParams.includingJobId,
    status: requestParams.status,
    updatedAtStart: requestParams.updatedAtStart,
    updatedAtEnd: requestParams.updatedAtEnd,
    pageSize,
  });

  return useInfiniteQuery(
    queryKey,
    async ({ pageParam = 0 }: { pageParam?: number }) => {
      return {
        data: await listJobsFor(
          {
            ...requestParams,
            includingJobId: pageParam > 0 ? undefined : requestParams.includingJobId,
            pagination: { pageSize, rowOffset: pageSize * pageParam },
          },
          requestOptions
        ),
        pageParam,
      };
    },
    {
      refetchInterval: (data) => {
        const jobStatus = data?.pages[0].data.jobs[0]?.job?.status;
        return jobStatus && jobStatusesIndicatingFinishedExecution.includes(jobStatus) ? 10000 : 2500;
      },
      getPreviousPageParam: (firstPage) => {
        return firstPage.pageParam > 0 ? firstPage.pageParam - 1 : undefined;
      },
      getNextPageParam: (lastPage, allPages) => {
        if (allPages.reduce((acc, page) => acc + page.data.jobs.length, 0) < lastPage.data.totalJobCount) {
          if (lastPage.pageParam === 0 && requestParams.includingJobId) {
            // the API will sometimes return more items than we request. If we include includingJobId, it will return as many pages as necessary to include the job with the given id. In this case, we cannot trust the pageParam, so we need to calculate the actual page number.
            const actualPageNumber = lastPage.data.jobs.length / pageSize - 1;
            return actualPageNumber + 1;
          }
          // all the data we have loaded is less than the total indicated by the API, so we can get another page
          return lastPage.pageParam + 1;
        }
        return undefined;
      },
    }
  );
};

export const useListJobsForConnectionStatus = (connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    jobsKeys.useListJobsForConnectionStatus(connectionId),
    () =>
      listJobsFor(
        {
          configId: connectionId,
          configTypes: ["sync", "reset_connection"],
          pagination: {
            // This is an arbitrary number. We have to look back at several jobs to determine the current status of the connection. Just knowing whether it's running or not is not sufficient, we want to know the status of the last completed job as well.
            pageSize: 3,
          },
        },
        requestOptions
      ),
    {
      refetchInterval: (data) => {
        return data?.jobs?.[0]?.job?.status &&
          jobStatusesIndicatingFinishedExecution.includes(data?.jobs?.[0]?.job?.status)
          ? 10000
          : 2500;
      },
    }
  );
};

export const useSetConnectionJobsData = (connectionId: string) => {
  const queryClient = useQueryClient();
  return (data: Updater<JobReadList | undefined, JobReadList>) =>
    queryClient.setQueriesData(jobsKeys.useListJobsForConnectionStatus(connectionId), data);
};

// A disabled useQuery that can be called manually to download job logs
export const useGetDebugInfoJobManual = (id: number) => {
  const requestOptions = useRequestOptions();
  return useQuery([SCOPE_WORKSPACE, "jobs", "getDebugInfo", id], () => getJobDebugInfo({ id }, requestOptions), {
    enabled: false,
  });
};

export const useCancelJob = () => {
  const requestOptions = useRequestOptions();
  const mutation = useMutation(["useCancelJob"], (id: number) => cancelJob({ id }, requestOptions));
  const activeMutationsCount = useIsMutating(["useCancelJob"]);

  return { ...mutation, isLoading: activeMutationsCount > 0 };
};

export const useJobInfoWithoutLogs = (id: number) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "jobs", "infoWithoutLogs", id],
    () => getJobInfoWithoutLogs({ id }, requestOptions),
    {
      refetchInterval: (data) => {
        // keep refetching data while the job is still running or hasn't ended too long ago.
        // We need some time after the last attempt has stopped, since logs
        // keep incoming for some time after the job has already been marked as finished.
        const lastAttemptEndTimestamp =
          data?.attempts.length && data?.attempts[data.attempts.length - 1].attempt.endedAt;
        // While no attempt ended timestamp exists yet (i.e. the job is still running) or it hasn't ended
        // more than 2 minutes (2 * 60 * 1000ms) ago, keep refetching
        return lastAttemptEndTimestamp && Date.now() - lastAttemptEndTimestamp * 1000 > 2 * 60 * 1000 ? false : 2500;
      },
    }
  );
};

export const useAttemptForJob = (jobId: number, attemptNumber: number) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "jobs", "attemptForJob", jobId, attemptNumber],
    () => getAttemptForJob({ jobId, attemptNumber }, requestOptions),
    {
      refetchInterval: (data) => {
        // keep refetching data while the job is still running or hasn't ended too long ago.
        // We need some time after the last attempt has stopped, since logs
        // keep incoming for some time after the job has already been marked as finished.
        const lastAttemptEndTimestamp = data?.attempt.endedAt;
        // While no attempt ended timestamp exists yet (i.e. the job is still running) or it hasn't ended
        // more than 2 minutes (2 * 60 * 1000ms) ago, keep refetching
        return lastAttemptEndTimestamp && Date.now() - lastAttemptEndTimestamp * 1000 > 2 * 60 * 1000 ? false : 2500;
      },
    }
  );
};
