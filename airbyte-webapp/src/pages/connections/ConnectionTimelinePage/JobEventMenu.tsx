import { Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { useCurrentConnection, useDonwnloadJobLogsFetchQuery } from "core/api";
import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { ModalOptions, ModalResult, useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { copyToClipboard } from "core/utils/clipboard";

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
  connection,
  attemptNumber,
  setFilterValue,
}: {
  openModal: <ResultType>(options: ModalOptions<ResultType>) => Promise<ModalResult<ResultType>>;
  jobId?: number;
  eventId?: string;
  connection: WebBackendConnectionRead;
  attemptNumber?: number;
  setFilterValue?: (filterName: keyof TimelineFilterValues, value: string) => void;
}) => {
  if (!jobId && !eventId) {
    return;
  }

  openModal({
    size: "full",
    title: <FormattedMessage id="jobHistory.logs.title" values={{ connectionName: connection.name }} />,
    content: () => (
      <DefaultErrorBoundary>
        <Suspense
          fallback={
            <div className={styles.modalLoading}>
              <Spinner />
              <Text>
                <FormattedMessage id="jobHistory.logs.loadingJob" />
              </Text>
            </div>
          }
        >
          <JobLogsModalContent jobId={jobId} attemptNumber={attemptNumber} eventId={eventId} connection={connection} />
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
  const { registerNotification } = useNotificationService();

  const downloadJobLogs = useDonwnloadJobLogsFetchQuery();

  const onChangeHandler = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case JobMenuOptions.OpenLogsModal:
        openJobLogsModal({
          openModal,
          jobId,
          eventId,
          connection,
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
        downloadJobLogs(connection.name, jobId);
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
