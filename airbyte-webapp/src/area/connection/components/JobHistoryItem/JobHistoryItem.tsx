import classNames from "classnames";
import { Suspense, useCallback, useRef } from "react";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { JobWithAttempts } from "area/connection/types/jobs";
import { buildAttemptLink, useAttemptLink } from "area/connection/utils/attemptLink";
import { getJobCreatedAt, isClearJob } from "area/connection/utils/jobs";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentConnection, useCurrentWorkspace, useGetDebugInfoJobManual } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { copyToClipboard } from "core/utils/clipboard";
import { trackError } from "core/utils/datadog";
import { downloadFile, FILE_TYPE_DOWNLOAD, fileizeString } from "core/utils/file";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./JobHistoryItem.module.scss";
import { JobStats } from "./JobStats";
import { JobStatusIcon } from "./JobStatusIcon";
import { JobStatusLabel } from "./JobStatusLabel";

interface JobHistoryItemProps {
  jobWithAttempts: JobWithAttempts;
}

enum ContextMenuOptions {
  OpenLogsModal = "OpenLogsModal",
  CopyLinkToJob = "CopyLinkToJob",
  DownloadLogs = "DownloadLogs",
}

export const JobHistoryItem: React.FC<JobHistoryItemProps> = ({ jobWithAttempts }) => {
  const { openModal } = useModalService();
  const attemptLink = useAttemptLink();
  const wrapperRef = useRef<HTMLDivElement>(null);
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { refetch: fetchJobLogs } = useGetDebugInfoJobManual(jobWithAttempts.job.id);
  const workspaceId = useCurrentWorkspaceId();
  const { name: workspaceName } = useCurrentWorkspace();
  const connection = useCurrentConnection();

  useEffectOnce(() => {
    if (attemptLink.jobId === String(jobWithAttempts.job.id)) {
      wrapperRef.current?.scrollIntoView();
      openJobLogsModal(attemptLink.attemptId);
    }
  });

  const openJobLogsModal = useCallback(
    (initialAttemptId?: number) => {
      openModal({
        size: "full",
        title: formatMessage({ id: "jobHistory.logs.title" }, { connectionName: connection.name }),
        content: () => (
          <DefaultErrorBoundary>
            <Suspense
              fallback={
                <div className={styles.jobHistoryItem__modalLoading}>
                  <Spinner />
                </div>
              }
            >
              <JobLogsModal
                connectionId={connection.connectionId}
                jobId={jobWithAttempts.job.id}
                initialAttemptId={initialAttemptId}
              />
            </Suspense>
          </DefaultErrorBoundary>
        ),
      });
    },
    [connection.connectionId, connection.name, formatMessage, jobWithAttempts.job.id, openModal]
  );

  const handleClick = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case ContextMenuOptions.DownloadLogs:
        const notificationId = `download-logs-${jobWithAttempts.job.id}`;
        registerNotification({
          type: "info",
          text: (
            <FlexContainer alignItems="center">
              <FormattedMessage id="jobHistory.logs.logDownloadPending" values={{ jobId: jobWithAttempts.job.id }} />
              <div className={styles.jobHistoryItem__spinnerContainer}>
                <LoadingSpinner />
              </div>
            </FlexContainer>
          ),
          id: notificationId,
          timeout: false,
        });
        // Promise.all() with a timeout is used to ensure that the notification is shown to the user for at least 1 second
        Promise.all([
          fetchJobLogs()
            .then(({ data }) => {
              if (!data) {
                throw new Error("No logs returned from server");
              }
              const file = new Blob(
                [
                  data.attempts
                    .flatMap((info, index) => [
                      `>> ATTEMPT ${index + 1}/${data.attempts.length}\n`,
                      ...info.logs.logLines,
                      `\n\n\n`,
                    ])
                    .join("\n"),
                ],
                {
                  type: FILE_TYPE_DOWNLOAD,
                }
              );
              downloadFile(file, fileizeString(`${workspaceName}-logs-${jobWithAttempts.job.id}.txt`));
            })
            .catch((e) => {
              trackError(e, { workspaceId, jobId: jobWithAttempts.job.id });
              registerNotification({
                type: "error",
                text: formatMessage(
                  {
                    id: "jobHistory.logs.logDownloadFailed",
                  },
                  { jobId: jobWithAttempts.job.id }
                ),
                id: `download-logs-error-${jobWithAttempts.job.id}`,
              });
            }),
          new Promise((resolve) => setTimeout(resolve, 1000)),
        ]).finally(() => {
          unregisterNotificationById(notificationId);
        });
        break;
      case ContextMenuOptions.OpenLogsModal:
        openJobLogsModal();
        break;
      case ContextMenuOptions.CopyLinkToJob:
        const url = new URL(window.location.href);
        url.hash = buildAttemptLink(
          jobWithAttempts.job.id,
          jobWithAttempts.attempts[jobWithAttempts.attempts.length - 1].id
        );
        copyToClipboard(url.href);
        registerNotification({
          type: "success",
          text: formatMessage({ id: "jobHistory.copyLinkToJob.success" }),
          id: "jobHistory.copyLinkToJob.success",
        });
        break;
    }
  };

  const streamsToList = isClearJob(jobWithAttempts)
    ? jobWithAttempts.job.resetConfig?.streamsToReset?.map((stream) => stream.name)
    : jobWithAttempts.job.refreshConfig?.streamsToRefresh?.map((stream) => stream.name);

  return (
    <div
      ref={wrapperRef}
      className={classNames(styles.jobHistoryItem, {
        [styles["jobHistoryItem--linked"]]: attemptLink.jobId === String(jobWithAttempts.job.id),
      })}
      id={String(jobWithAttempts.job.id)}
    >
      <Box pr="xl">
        <FlexContainer justifyContent="center" alignItems="center">
          <JobStatusIcon job={jobWithAttempts} />
        </FlexContainer>
      </Box>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.jobHistoryItem__main}>
        <Box className={styles.jobHistoryItem__summary}>
          <JobStatusLabel jobWithAttempts={jobWithAttempts} />
          {isClearJob(jobWithAttempts) || jobWithAttempts.job.configType === "refresh" ? (
            <ResetStreamsDetails names={streamsToList} />
          ) : (
            <JobStats jobWithAttempts={jobWithAttempts} />
          )}
        </Box>
        <Box pr="lg" className={styles.jobHistoryItem__timestamp}>
          <Text>
            <FormattedDate value={getJobCreatedAt(jobWithAttempts) * 1000} dateStyle="medium" timeStyle="short" />
          </Text>
        </Box>
      </FlexContainer>
      <DropdownMenu
        placement="bottom-end"
        options={[
          {
            displayName: formatMessage({ id: "jobHistory.copyLinkToJob" }),
            value: ContextMenuOptions.CopyLinkToJob,
          },
          {
            displayName: formatMessage({ id: "jobHistory.viewLogs" }),
            value: ContextMenuOptions.OpenLogsModal,
            disabled: jobWithAttempts.attempts.length === 0,
          },
          {
            displayName: formatMessage({ id: "jobHistory.downloadLogs" }),
            value: ContextMenuOptions.DownloadLogs,
            disabled: jobWithAttempts.attempts.length === 0,
          },
        ]}
        onChange={handleClick}
      >
        {() => <Button variant="clear" icon="options" />}
      </DropdownMenu>
    </div>
  );
};
