import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { Suspense, useRef } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { ConnectionStatusLoadingSpinner } from "components/connection/ConnectionStatusIndicator";
import { buildAttemptLink, useAttemptLink } from "components/JobItem/attemptLinkUtils";
import { AttemptDetails } from "components/JobItem/components/AttemptDetails";
import { getJobCreatedAt } from "components/JobItem/components/JobSummary";
import { ResetStreamsDetails } from "components/JobItem/components/ResetStreamDetails";
import { JobWithAttempts } from "components/JobItem/types";
import { getJobAttempts } from "components/JobItem/utils";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useGetDebugInfoJobManual } from "core/api";
import { copyToClipboard } from "core/utils/clipboard";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { JobLogsModalContent } from "./JobLogsModalContent";
import { JobStatusIcon } from "./JobStatusIcon";
import { JobStatusLabel } from "./JobStatusLabel";
import styles from "./NewJobItem.module.scss";

interface NewJobItemProps {
  jobWithAttempts: JobWithAttempts;
}

enum ContextMenuOptions {
  OpenLogsModal = "OpenLogsModal",
  CopyLinkToJob = "CopyLinkToJob",
  DownloadLogs = "DownloadLogs",
}

export const NewJobItem: React.FC<NewJobItemProps> = ({ jobWithAttempts }) => {
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
    }
  });

  const handleClick = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case ContextMenuOptions.DownloadLogs:
        const notificationId = `download-logs-${jobWithAttempts.job.id}`;
        registerNotification({
          type: "info",
          text: (
            <FlexContainer alignItems="center">
              <FormattedMessage id="jobHistory.logs.logDownloadPending" values={{ jobId: jobWithAttempts.job.id }} />
              <ConnectionStatusLoadingSpinner />
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
        openModal({
          size: "full",
          title: formatMessage(
            { id: "jobHistory.logs.title" },
            { jobId: jobWithAttempts.job.id, connectionName: connection.name }
          ),
          content: () => (
            <Suspense
              fallback={
                <div className={styles.newJobItem__modalLoading}>
                  <Spinner />
                </div>
              }
            >
              <JobLogsModalContent jobId={jobWithAttempts.job.id} />
            </Suspense>
          ),
        });
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
      className={classNames(styles.newJobItem, {
        [styles["newJobItem--linked"]]: attemptLink.jobId === String(jobWithAttempts.job.id),
      })}
      id={String(jobWithAttempts.job.id)}
    >
      <Box pr="xl">
        <JobStatusIcon job={jobWithAttempts} />
      </Box>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.newJobItem__main}>
        <Box className={styles.newJobItem__summary}>
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
              />
            )
          )}
        </Box>
        <Box pr="lg" className={styles.newJobItem__timestamp}>
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
        {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
      </DropdownMenu>
    </div>
  );
};
