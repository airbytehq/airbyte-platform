import React, { useEffect, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { downloadJobLogLines, getJobLogLines, useOpenJobLogsModal } from "area/connection/components/JobFailure";
import { FailureReason, JobConfigType, LogEvents, LogRead } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";
import { copyToClipboard } from "core/utils/clipboard";
import { trackError } from "core/utils/datadog";

import styles from "./TestFailurePanel.module.scss";

const COPIED_FEEDBACK_DURATION_MS = 2500;

type FormatMessage = ReturnType<typeof useIntl>["formatMessage"];

/** The submit step that failed: the connection test, or a save. */
export type FailedStep = "test" | "save";

const TITLE_ID_BY_FAILED_STEP: Record<FailedStep, string> = {
  test: "connectorForm.testFailure.title",
  save: "connectorForm.saveFailure.title",
};

interface TestFailurePanelProps {
  failedStep: FailedStep;
  /** Shown when the check returned no external message, for example when the check request itself failed. */
  fallbackMessage?: React.ReactNode;
  jobId?: string;
  jobConfigType?: JobConfigType;
  jobLogs?: LogEvents | LogRead;
  failureReason?: FailureReason;
  onClose: () => void;
}

// A message built from React elements, such as the generic error text, has no plain text to copy
const toPlainText = (node: React.ReactNode) => (typeof node === "string" ? node : undefined);

const formatDetail = (label: string, value: string | undefined) => (value ? `${label} ${value}` : undefined);

const getFailureDetailsText = (
  messageText: string | undefined,
  failureReason: FailureReason | undefined,
  formatMessage: FormatMessage
) =>
  [
    messageText,
    formatDetail(formatMessage({ id: "jobs.failure.internalMessageLabel" }), failureReason?.internalMessage),
    formatDetail(formatMessage({ id: "jobs.failure.originLabel" }), failureReason?.failureOrigin),
    formatDetail(formatMessage({ id: "jobs.failure.typeLabel" }), failureReason?.failureType),
    formatDetail(formatMessage({ id: "connectorForm.testFailure.stacktraceLabel" }), failureReason?.stacktrace),
  ]
    .filter(Boolean)
    .join("\n");

const CopyErrorButton: React.FC<{ errorText: string }> = ({ errorText }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const [isCopied, setIsCopied] = useState(false);

  useEffect(() => {
    if (!isCopied) {
      return;
    }
    const timeout = setTimeout(() => setIsCopied(false), COPIED_FEEDBACK_DURATION_MS);
    return () => clearTimeout(timeout);
  }, [isCopied]);

  const copyError = async () => {
    try {
      await copyToClipboard(errorText);
      setIsCopied(true);
    } catch (e) {
      trackError(e, { id: "connectorForm.testFailure.copyError" });
      registerNotification({
        id: "connectorForm.testFailure.copyError.failure",
        type: "error",
        text: formatMessage({ id: "connectorForm.testFailure.copyError.failure" }),
      });
    }
  };

  return (
    <Button type="button" variant="link" onClick={copyError}>
      <FormattedMessage
        id={isCopied ? "connectorForm.testFailure.errorCopied" : "connectorForm.testFailure.copyError"}
      />
    </Button>
  );
};

const LogsButtons: React.FC<{ jobId: string; configType: JobConfigType; logs: LogEvents | LogRead }> = ({
  jobId,
  configType,
  logs,
}) => {
  const openJobLogsModal = useOpenJobLogsModal();

  return (
    <>
      <Button type="button" variant="link" onClick={() => openJobLogsModal(configType, logs)}>
        <FormattedMessage id="jobHistory.viewLogs" />
      </Button>
      <Button type="button" variant="link" onClick={() => downloadJobLogLines(jobId, configType, getJobLogLines(logs))}>
        <FormattedMessage id="sources.downloadLogs" />
      </Button>
    </>
  );
};

export const TestFailurePanel: React.FC<TestFailurePanelProps> = ({
  failedStep,
  fallbackMessage,
  jobId,
  jobConfigType,
  jobLogs,
  failureReason,
  onClose,
}) => {
  const { formatMessage } = useIntl();
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    panelRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, []);

  const message = failureReason?.externalMessage || fallbackMessage || <FormattedMessage id="form.someError" />;
  const errorText = getFailureDetailsText(toPlainText(message), failureReason, formatMessage);

  return (
    <FlexContainer direction="column" ref={panelRef} data-testid="connector-test-failure">
      <FlexContainer alignItems="center" justifyContent="space-between">
        <Text as="h3" size="lg">
          <FormattedMessage id={TITLE_ID_BY_FAILED_STEP[failedStep]} />
        </Text>
        <Button
          type="button"
          variant="clear"
          size="xs"
          icon="cross"
          iconColor="affordance"
          aria-label={formatMessage({ id: "connectorForm.testFailure.close" })}
          onClick={onClose}
        />
      </FlexContainer>
      <Message
        type="error"
        className={styles.errorBox}
        text={null}
        secondaryText={
          <>
            <span className={styles.message}>{message}</span>
            <span className={styles.actions}>
              <CopyErrorButton errorText={errorText} />
              {jobLogs && jobConfigType && jobId && (
                <LogsButtons jobId={jobId} configType={jobConfigType} logs={jobLogs} />
              )}
            </span>
          </>
        }
      />
    </FlexContainer>
  );
};
