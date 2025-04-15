import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { useIntl } from "react-intl";

import { formatLogEvent } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { trackError } from "core/utils/datadog";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";
import { useNotificationService } from "hooks/services/Notification";

import { useCurrentWorkspace } from "./workspaces";
import {
  cancelJob,
  getAttemptCombinedStats,
  getAttemptForJob,
  getJobDebugInfo,
  getJobInfoWithoutLogs,
  explainJob,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { AttemptInfoRead, AttemptRead, LogEvents, LogRead } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const jobsKeys = {
  all: (connectionId: string | undefined) => [SCOPE_WORKSPACE, connectionId] as const,
  explain: (jobId: number) => [SCOPE_WORKSPACE, "jobs", "explain", jobId] as const,
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

type AttemptInfoReadWithFormattedLogs = AttemptInfoRead & { logType: "formatted"; logs: LogRead };
type AttemptInfoReadWithStructuredLogs = AttemptInfoRead & { logType: "structured"; logs: LogEvents };
export type AttemptInfoReadWithLogs = AttemptInfoReadWithFormattedLogs | AttemptInfoReadWithStructuredLogs;

export function attemptHasFormattedLogs(attempt: AttemptInfoRead): attempt is AttemptInfoReadWithFormattedLogs {
  return attempt.logType === "formatted";
}

export function attemptHasStructuredLogs(attempt: AttemptInfoRead): attempt is AttemptInfoReadWithStructuredLogs {
  return attempt.logType === "structured";
}

export const useAttemptForJob = (jobId: number, attemptNumber: number) => {
  const requestOptions = useRequestOptions();
  return useQuery(
    [SCOPE_WORKSPACE, "jobs", "attemptForJob", jobId, attemptNumber],
    () => getAttemptForJob({ jobId, attemptNumber }, requestOptions) as Promise<AttemptInfoReadWithLogs>,
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

export const useAttemptCombinedStatsForJob = (jobId: number, attempt: AttemptRead) => {
  const requestOptions = useRequestOptions();
  return useQuery(
    [SCOPE_WORKSPACE, "jobs", "attemptCombinedStatsForJob", jobId, attempt.id],
    () => getAttemptCombinedStats({ jobId, attemptNumber: attempt.id }, requestOptions),
    {
      refetchInterval: () => {
        // if the attempt hasn't ended refetch every 2.5 seconds
        return attempt.endedAt ? false : 2500;
      },
    }
  );
};

export const useDonwnloadJobLogsFetchQuery = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const workspace = useCurrentWorkspace();
  const { formatMessage } = useIntl();

  return useCallback(
    (connectionName: string, jobId: number) => {
      // Promise.all() with a timeout is used to ensure that the notification is shown to the user for at least 1 second
      queryClient.fetchQuery({
        queryKey: [SCOPE_WORKSPACE, "jobs", "getDebugInfo", jobId],
        queryFn: async () => {
          const notificationId = `download-logs-${jobId}`;
          registerNotification({
            type: "info",
            text: formatMessage(
              {
                id: "jobHistory.logs.logDownloadPending",
              },
              { jobId }
            ),
            id: notificationId,
            timeout: false,
          });
          try {
            return await Promise.all([
              getJobDebugInfo({ id: jobId }, requestOptions)
                .then((data) => {
                  if (!data) {
                    throw new Error("No logs returned from server");
                  }
                  const file = new Blob(
                    [
                      data.attempts
                        .flatMap((info, index) => [
                          `>> ATTEMPT ${index + 1}/${data.attempts.length}\n`,
                          ...(attemptHasFormattedLogs(info) ? info.logs.logLines : []),
                          ...(attemptHasStructuredLogs(info)
                            ? info.logs.events.map((event) => formatLogEvent(event))
                            : []),
                          `\n\n\n`,
                        ])
                        .join("\n"),
                    ],
                    {
                      type: FILE_TYPE_DOWNLOAD,
                    }
                  );
                  downloadFile(file, fileizeString(`${connectionName}-logs-${jobId}.txt`));
                })
                .catch((e) => {
                  trackError(e, { workspaceId: workspace.workspaceId, jobId });
                  registerNotification({
                    type: "error",
                    text: formatMessage({
                      id: "jobHistory.logs.logDownloadFailed",
                    }),
                    id: `download-logs-error-${jobId}`,
                  });
                }),
              new Promise((resolve) => setTimeout(resolve, 1000)),
            ]);
          } finally {
            unregisterNotificationById(notificationId);
          }
        },
      });
    },
    [
      formatMessage,
      queryClient,
      registerNotification,
      requestOptions,
      unregisterNotificationById,
      workspace.workspaceId,
    ]
  );
};

export const useExplainJob = (jobId: number) => {
  const requestOptions = useRequestOptions();

  return useQuery({
    queryFn: () => explainJob({ id: jobId }, requestOptions),
    queryKey: jobsKeys.explain(jobId),
    staleTime: Infinity,
    cacheTime: Infinity,
  });
};
