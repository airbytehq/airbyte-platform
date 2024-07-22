import { Suspense } from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Spinner } from "components/ui/Spinner";

import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { copyToClipboard } from "core/utils/clipboard";
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
  registerNotification: (notification: Notification) => void
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
  }
};

export const JobEventMenu: React.FC<{ eventId: string; jobId: number }> = ({ eventId, jobId }) => {
  const { formatMessage } = useIntl();
  const { connection } = useConnectionFormService();
  const { openModal } = useModalService();
  const { registerNotification } = useNotificationService();

  if (!jobId) {
    return null;
  }

  const onChangeHandler = (optionClicked: DropdownMenuOptionType) => {
    handleClick(optionClicked, connection.name ?? "", formatMessage, eventId, jobId, openModal, registerNotification);
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
      ]}
      onChange={onChangeHandler}
    >
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
