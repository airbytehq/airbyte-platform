import { Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Spinner } from "components/ui/Spinner";

import {
  attemptHasFormattedLogs,
  attemptHasStructuredLogs,
  useCurrentConnection,
  useCurrentWorkspace,
  useGetDebugInfoJobManual,
} from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { copyToClipboard } from "core/utils/clipboard";
import { trackError } from "core/utils/datadog";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";
import { ModalOptions, ModalResult, useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./JobEventMenu.module.scss";
import { JobLogsModalContent } from "./JobLogsModalContent";
import { TimelineFilterValues } from "./utils";

enum JobMenuOptions {
  OpenLogsModal = "OpenLogsModal",
  CopyLinkToEvent = "CopyLinkToEvent",
  DownloadLogs = "DownloadLogs",
}

export const openJobLogsModal = ({
  openModal,
  jobId,
  eventId,
  connectionName,
  attemptNumber,
  connectionId,
  setFilterValue,
}: {
  openModal: <ResultType>(options: ModalOptions<ResultType>) => Promise<ModalResult<ResultType>>;
  jobId?: number;
  eventId?: string;
  connectionName: string;
  attemptNumber?: number;
  connectionId: string;
  setFilterValue?: (filterName: keyof TimelineFilterValues, value: string) => void;
}) => {
  if (!jobId && !eventId) {
    return;
  }

  openModal({
    size: "full",
    title: <FormattedMessage id="jobHistory.logs.title" values={{ connectionName }} />,
    content: () => (
      <DefaultErrorBoundary>
        <Suspense
          fallback={
            <div className={styles.modalLoading}>
              <Spinner />
            </div>
          }
        >
          <JobLogsModalContent
            jobId={jobId}
            attemptNumber={attemptNumber}
            eventId={eventId}
            connectionId={connectionId}
          />
        </Suspense>
      </DefaultErrorBoundary>
    ),
  }).then((result) => {
    if (result && setFilterValue) {
      setFilterValue("openLogs", "");
    }
  });
};

export const JobEventMenu: React.FC<{ eventId?: string; jobId: number; attemptCount?: number }> = ({
  eventId,
  jobId,
  attemptCount,
}) => {
  const { formatMessage } = useIntl();
  const connection = useCurrentConnection();
  const { openModal } = useModalService();
  const { registerNotification, unregisterNotificationById } = useNotificationService();

  const { refetch: fetchJobLogs } = useGetDebugInfoJobManual(jobId);
  const { name: workspaceName, workspaceId } = useCurrentWorkspace();

  const onChangeHandler = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case JobMenuOptions.OpenLogsModal:
        openJobLogsModal({
          openModal,
          jobId,
          eventId,
          connectionName: connection.name,
          connectionId: connection.connectionId,
        });
        break;

      case JobMenuOptions.CopyLinkToEvent: {
        const url = new URL(window.location.href);
        if (eventId) {
          url.searchParams.set("eventId", eventId);
        } else {
          url.searchParams.set("jobId", jobId.toString());
        }
        url.searchParams.set("openLogs", "true");

        copyToClipboard(url.href);
        registerNotification({
          type: "success",
          text: formatMessage({ id: "jobHistory.copyLinkToEvent.success" }),
          id: "jobHistory.copyLinkToEvent.success",
        });
        break;
      }

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
                      ...(attemptHasFormattedLogs(info) ? info.logs.logLines : []),
                      ...(attemptHasStructuredLogs(info) ? info.logs.events.map((event) => JSON.stringify(event)) : []),
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
                  { connectionName: connection.name }
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

  return (
    <DropdownMenu
      placement="bottom-end"
      data-testid="job-event-menu"
      options={[
        {
          displayName: formatMessage({
            id: "jobHistory.copyLinkToEvent",
          }),
          value: JobMenuOptions.CopyLinkToEvent,
          "data-testid": "copy-link-to-event",
        },
        {
          displayName: formatMessage({ id: "jobHistory.viewLogs" }),
          value: JobMenuOptions.OpenLogsModal,
          disabled: attemptCount === 0,
          "data-testid": "view-logs",
        },
        {
          displayName: formatMessage({ id: "jobHistory.downloadLogs" }),
          value: JobMenuOptions.DownloadLogs,
          disabled: attemptCount === 0,
          "data-testid": "download-logs",
        },
      ]}
      onChange={onChangeHandler}
    >
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
