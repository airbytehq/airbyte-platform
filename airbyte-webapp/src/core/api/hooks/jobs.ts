import { Updater, useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useExperiment } from "hooks/services/Experiment";

import {
  cancelJob,
  getAttemptForJob,
  getJobDebugInfo,
  getJobInfoWithoutLogs,
  listJobsFor,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { JobListRequestBody, JobReadList, JobStatus } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListJobs = (listParams: JobListRequestBody, keepPreviousData = true) => {
  const requestOptions = useRequestOptions();
  const queryKey = [
    SCOPE_WORKSPACE,
    "jobs",
    "list",
    listParams.configId,
    listParams.includingJobId,
    listParams.pagination,
  ];

  const result = useQuery(queryKey, () => listJobsFor(listParams, requestOptions), {
    refetchInterval: (data) => {
      return data?.jobs?.[0]?.job?.status === JobStatus.running ? 2500 : 10000;
    },
    keepPreviousData,
    suspense: true,
  });

  return {
    data: result.data as JobReadList, // cast to JobReadList because (suspense: true) means we will never get undefined
    isPreviousData: result.isPreviousData,
  };
};

export const useListJobsForConnectionStatus = (connectionId: string) => {
  return useListJobs({
    configId: connectionId,
    configTypes: ["sync", "reset_connection"],
    pagination: {
      pageSize: useExperiment("connection.streamCentricUI.numberOfLogsToLoad", 15),
    },
  });
};

export const useSetConnectionJobsData = (connectionId: string) => {
  const queryClient = useQueryClient();
  return (data: Updater<JobReadList | undefined, JobReadList>) =>
    queryClient.setQueriesData([SCOPE_WORKSPACE, "jobs", "list", connectionId], data);
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
