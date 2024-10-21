import { UseQueryOptions, useIsMutating, useMutation, useQuery } from "@tanstack/react-query";

import { jobStatusesIndicatingFinishedExecution } from "components/connection/ConnectionSync/ConnectionSyncContext";

import {
  cancelJob,
  getAttemptCombinedStats,
  getAttemptForJob,
  getJobDebugInfo,
  getJobInfoWithoutLogs,
  listJobsFor,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { AttemptStats } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const jobsKeys = {
  all: (connectionId: string | undefined) => [SCOPE_WORKSPACE, connectionId] as const,
  useListJobsForConnectionStatus: (connectionId: string) =>
    [...jobsKeys.all(connectionId), "connectionStatus"] as const,
};

export const useListJobsForConnectionStatus = (connectionId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    jobsKeys.useListJobsForConnectionStatus(connectionId),
    () =>
      listJobsFor(
        {
          configId: connectionId,
          configTypes: ["sync", "reset_connection", "clear", "refresh"],
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

export const useAttemptCombinedStatsForJob = (
  jobId: number,
  attemptNumber: number,
  options?: Readonly<Omit<UseQueryOptions<AttemptStats>, "queryKey" | "queryFn" | "suspense">>
) => {
  const requestOptions = useRequestOptions();
  // the endpoint returns a 404 if there aren't stats for this attempt
  try {
    return useSuspenseQuery(
      [SCOPE_WORKSPACE, "jobs", "attemptCombinedStatsForJob", jobId, attemptNumber],
      () => getAttemptCombinedStats({ jobId, attemptNumber }, requestOptions),
      options
    );
  } catch (e) {
    return undefined;
  }
};
