import { QueryObserverResult } from "@tanstack/react-query";
import { Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Spinner } from "components/ui/Spinner";

import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { useCurrentWorkspace, useGetDebugInfoJobManual } from "core/api";
import { JobDebugInfoRead } from "core/api/types/AirbyteClient";
import { copyToClipboard } from "core/utils/clipboard";
import { trackError } from "core/utils/datadog";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { ModalOptions, ModalResult, useModalService } from "hooks/services/Modal";
import { Notification, useNotificationService } from "hooks/services/Notification";

import styles from "./JobEventMenu.module.scss";

enum JobMenuOptions {
  OpenLogsModal = "OpenLogsModal",
  CopyLinkToJob = "CopyLinkToJob",
  DownloadLogs = "DownloadLogs",
}
export const openJobLogsModalFromTimeline = ({
  openModal,
  jobId,
  formatMessage,
  connectionName,
  initialAttemptId,
}: {
  openModal: <ResultType>(options: ModalOptions<ResultType>) => Promise<ModalResult<ResultType>>;
  jobId: number;
  formatMessage: (arg0: { id: string }, arg1?: { connectionName: string } | undefined) => string;
  connectionName: string;
  initialAttemptId?: number;
}) => {
  openModal({
    size: "full",
    title: formatMessage({ id: "jobHistory.logs.title" }, { connectionName }),
    content: () => (
      <Suspense
        fallback={
          <div className={styles.modalLoading}>
            <Spinner />
          </div>
        }
      >
        <JobLogsModal jobId={jobId} initialAttemptId={initialAttemptId} />
      </Suspense>
    ),
  });
};

const handleClick = (
  optionClicked: DropdownMenuOptionType,
  connectionName: string,
  formatMessage: (arg0: { id: string }, arg1?: { connectionName: string } | undefined) => string,
  eventId: string,
  jobId: number,
  openModal: <ResultType>(options: ModalOptions<ResultType>) => Promise<ModalResult<ResultType>>,
  registerNotification: (notification: Notification) => void,
  unregisterNotificationById: (id: string) => void,
  fetchJobLogs: () => Promise<QueryObserverResult<JobDebugInfoRead, unknown>>,
  workspaceName: string,
  workspaceId: string
) => {
  switch (optionClicked.value) {
    case JobMenuOptions.OpenLogsModal:
      openJobLogsModalFromTimeline({ openModal, jobId, formatMessage, connectionName });
      break;

    case JobMenuOptions.CopyLinkToJob:
      const url = new URL(window.location.href);
      url.searchParams.set("eventId", eventId);
      url.searchParams.set("openLogs", "true");

      copyToClipboard(url.href);
      registerNotification({
        type: "success",
        text: formatMessage({ id: "jobHistory.copyLinkToJob.success" }),
        id: "jobHistory.copyLinkToJob.success",
      });
      break;
    case JobMenuOptions.DownloadLogs:
      const notificationId = `download-logs-${jobId}`;
      registerNotification({
        type: "info",
        text: (
          <FlexContainer alignItems="center">
            <FormattedMessage id="jobHistory.logs.logDownloadPending" values={{ jobId }} />
            <div className={styles.spinnerContainer}>
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
            downloadFile(file, fileizeString(`${workspaceName}-logs-${jobId}.txt`));
          })
          .catch((e) => {
            trackError(e, { workspaceId, jobId });
            registerNotification({
              type: "error",
              text: formatMessage(
                {
                  id: "jobHistory.logs.logDownloadFailed",
                },
                { connectionName }
              ),
              id: `download-logs-error-${jobId}`,
            });
          }),
        new Promise((resolve) => setTimeout(resolve, 1000)),
      ]).finally(() => {
        unregisterNotificationById(notificationId);
      });
      break;
  }
};

export const JobEventMenu: React.FC<{ eventId: string; jobId: number }> = ({ eventId, jobId }) => {
  const { formatMessage } = useIntl();
  const { connection } = useConnectionFormService();
  const { openModal } = useModalService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { refetch: fetchJobLogs } = useGetDebugInfoJobManual(jobId);
  const { name: workspaceName, workspaceId } = useCurrentWorkspace();

  if (!jobId) {
    return null;
  }

  const onChangeHandler = (optionClicked: DropdownMenuOptionType) => {
    handleClick(
      optionClicked,
      connection.name ?? "",
      formatMessage,
      eventId,
      jobId,
      openModal,
      registerNotification,
      unregisterNotificationById,
      fetchJobLogs,
      workspaceName,
      workspaceId
    );
  };

  return (
    <DropdownMenu
      placement="bottom-end"
      options={[
        {
          displayName: formatMessage({ id: "jobHistory.copyLinkToJob" }),
          value: JobMenuOptions.CopyLinkToJob,
        },
        {
          displayName: formatMessage({ id: "jobHistory.viewLogs" }),
          value: JobMenuOptions.OpenLogsModal,
        },
        {
          displayName: formatMessage({ id: "jobHistory.downloadLogs" }),
          value: JobMenuOptions.DownloadLogs,
        },
      ]}
      onChange={onChangeHandler}
    >
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
