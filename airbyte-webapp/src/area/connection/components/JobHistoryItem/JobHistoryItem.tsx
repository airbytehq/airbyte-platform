import classNames from "classnames";
import { Suspense, useCallback, useRef } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { AttemptDetails } from "area/connection/components/AttemptDetails";
import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { JobWithAttempts } from "area/connection/types/jobs";
import { buildAttemptLink, useAttemptLink } from "area/connection/utils/attemptLink";
import { isJobPartialSuccess, getJobAttempts, getJobCreatedAt } from "area/connection/utils/jobs";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useGetDebugInfoJobManual } from "core/api";
import { copyToClipboard } from "core/utils/clipboard";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./JobHistoryItem.module.scss";
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
  const attempts = getJobAttempts(jobWithAttempts);
  const attemptLink = useAttemptLink();
  const wrapperRef = useRef<HTMLDivElement>(null);
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { refetch: fetchJobLogs } = useGetDebugInfoJobManual(jobWithAttempts.job.id);
  const workspaceId = useCurrentWorkspaceId();
  const { name: workspaceName } = useCurrentWorkspace();
  const { trackError } = useAppMonitoringService();
  const { connection } = useConnectionEditService();

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
          <Suspense
            fallback={
              <div className={styles.jobHistoryItem__modalLoading}>
                <Spinner />
              </div>
            }
          >
            <JobLogsModal jobId={jobWithAttempts.job.id} initialAttemptId={initialAttemptId} />
          </Suspense>
        ),
      });
    },
    [connection.name, formatMessage, jobWithAttempts.job.id, openModal]
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

  return (
    <div
      ref={wrapperRef}
      className={classNames(styles.jobHistoryItem, {
        [styles["jobHistoryItem--linked"]]: attemptLink.jobId === String(jobWithAttempts.job.id),
      })}
      id={String(jobWithAttempts.job.id)}
    >
      <Box pr="xl">
        <JobStatusIcon job={jobWithAttempts} />
      </Box>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.jobHistoryItem__main}>
        <Box className={styles.jobHistoryItem__summary}>
          <JobStatusLabel jobWithAttempts={jobWithAttempts} />
          {jobWithAttempts.job.configType === "reset_connection" ? (
            <ResetStreamsDetails
              names={jobWithAttempts.job.resetConfig?.streamsToReset?.map((stream) => stream.name)}
            />
          ) : (
            attempts &&
            attempts.length > 0 && (
              <AttemptDetails
                attempt={attempts[attempts.length - 1]}
                hasMultipleAttempts={attempts.length > 1}
                jobId={String(jobWithAttempts.job.id)}
                isPartialSuccess={isJobPartialSuccess(jobWithAttempts.attempts)}
              />
            )
          )}
        </Box>
        <Box pr="lg" className={styles.jobHistoryItem__timestamp}>
          <Text>
            <FormattedTimeParts value={getJobCreatedAt(jobWithAttempts) * 1000} hour="numeric" minute="2-digit">
              {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
            </FormattedTimeParts>
            <FormattedDate
              value={getJobCreatedAt(jobWithAttempts) * 1000}
              month="2-digit"
              day="2-digit"
              year="numeric"
            />
          </Text>
          {jobWithAttempts.attempts.length > 1 && (
            <Box mt="xs">
              <Text size="sm" color="grey" align="right">
                <FormattedMessage id="sources.countAttempts" values={{ count: jobWithAttempts.attempts.length }} />
              </Text>
            </Box>
          )}
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
        {() => <Button variant="clear" icon={<Icon type="options" />} />}
      </DropdownMenu>
    </div>
  );
};
