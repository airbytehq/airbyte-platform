import classNames from "classnames";
import { Suspense, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useToggle } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import Logs from "components/ui/Logs";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import {
  CleanedLogLines,
  formatLogEvent,
  formatLogEventTimestamp,
} from "area/connection/components/JobHistoryItem/useCleanLogs";
import { VirtualLogs } from "area/connection/components/JobHistoryItem/VirtualLogs";
import { areLogsFormatted, areLogsStructured } from "area/connection/utils/jobs";
import { FailureReason, JobConfigType, LogEvents, LogRead, SynchronousJobRead } from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { useModalService } from "core/services/Modal";
import { downloadFile } from "core/utils/file";

import styles from "./JobFailure.module.scss";

export interface JobFailureProps {
  job?: SynchronousJobRead;
  fallbackMessage?: React.ReactNode;
  id?: string;
  configType?: JobConfigType;
  logs?: LogEvents | LogRead;
  failureReason?: FailureReason;
}

/**
 * Hook that transforms SynchronousJobRead logs into CleanedLogLines format
 * Similar to useCleanLogs but specifically for SynchronousJobRead
 */
const useCleanJobLogs = (jobLogs: LogEvents | LogRead): CleanedLogLines => {
  return useMemo(() => {
    if (areLogsFormatted(jobLogs)) {
      return jobLogs.logLines.map((line, index) => ({
        lineNumber: index + 1,
        original: line,
        text: line,
      }));
    }

    if (areLogsStructured(jobLogs) && jobLogs.events) {
      return jobLogs.events.map((event, index) => ({
        lineNumber: index + 1,
        original: JSON.stringify(event),
        text: formatLogEvent(event),
        timestamp: formatLogEventTimestamp(event.timestamp),
        level: event.level,
      }));
    }

    return [];
  }, [jobLogs]);
};

const JobLogsModalContent = ({ jobLogs }: { jobLogs: LogEvents | LogRead }) => {
  const logLines = useCleanJobLogs(jobLogs);

  return (
    <div className={styles.logsModal}>
      <VirtualLogs attemptId={1} logLines={logLines} hasFailure showStructuredLogs={areLogsStructured(jobLogs)} />
    </div>
  );
};

function getTitlePhraseIdForJobConfigType(jobConfigType: JobConfigType) {
  switch (jobConfigType) {
    case "check_connection_source":
      return "jobs.jobStatus.check_connection_source.failed";
    case "check_connection_destination":
      return "jobs.jobStatus.check_connection_destination.failed";
    case "discover_schema":
      return "jobs.jobStatus.discover_schema.failed";
  }
  throw new Error(`Job type ${jobConfigType} not supported`);
}

const DisclosureHeader = ({
  toggle,
  expanded,
  messageId,
  icon,
}: {
  toggle: () => void;
  expanded: boolean;
  messageId: string;
  icon?: React.ReactNode;
}) => {
  return (
    <FlexContainer alignItems="center">
      <button type="button" onClick={toggle} className={classNames(styles.label)}>
        <FlexContainer alignItems="center" gap="none">
          <FlexItem className={styles.toggleIcon}>
            {expanded ? <Icon type="chevronDown" /> : <Icon type="chevronRight" />}
          </FlexItem>
          <FlexItem grow>
            <FormattedMessage id={messageId} />
          </FlexItem>
        </FlexContainer>
      </button>
      {icon && <FlexItem className={styles.iconContainer}>{icon}</FlexItem>}
    </FlexContainer>
  );
};

const DownloadButton = ({
  id,
  configType,
  logLines,
}: {
  id: string;
  configType: JobConfigType;
  logLines: string[];
}) => {
  const { formatMessage } = useIntl();

  const downloadFileWithLogs = () => {
    const file = new Blob([logLines.join("\n")], {
      type: "text/plain;charset=utf-8",
    });
    downloadFile(file, `${configType}-failure-${id}.txt`);
  };

  return (
    <Button
      variant="secondary"
      onClick={downloadFileWithLogs}
      type="button"
      title={formatMessage({
        id: "sources.downloadLogs",
      })}
      icon="download"
    />
  );
};

export const JobFailure: React.FC<JobFailureProps> = (props) => {
  const configType = props.job ? props.job.configType : props.configType;
  const jobId = props.job ? props.job.id : props.id;
  const logs = props.job ? props.job.logs : props.logs;
  const failureReason = props.job ? props.job.failureReason : props.failureReason;
  const { fallbackMessage } = props;

  const [isDetailsExpanded, toggleDetails] = useToggle(false);
  const [isStacktraceExpanded, toggleStacktrace] = useToggle(false);
  const { openModal } = useModalService();

  if (!configType || !jobId) {
    return <Message text={fallbackMessage || <FormattedMessage id="form.someError" />} type="error" />;
  }

  const openLogsModal = () => {
    openModal({
      size: "full",
      title: <FormattedMessage id="jobHistory.logs.title" values={{ connectionName: `${configType} job` }} />,
      content: () => (
        <DefaultErrorBoundary>
          <Suspense fallback={<LoadingSpinner />}>{logs && <JobLogsModalContent jobLogs={logs} />}</Suspense>
        </DefaultErrorBoundary>
      ),
    });
  };

  const hasLogs = !!logs;

  const hasFailureDetails =
    failureReason && (failureReason.internalMessage || failureReason.failureOrigin || failureReason.failureType);

  return (
    <Message
      type="error"
      text={<FormattedMessage id={getTitlePhraseIdForJobConfigType(configType)} />}
      secondaryText={failureReason?.externalMessage || fallbackMessage}
    >
      {(hasLogs || hasFailureDetails || failureReason?.stacktrace) && (
        <FlexContainer direction="column" gap="sm">
          {hasLogs && (
            <FlexItem>
              <FlexContainer alignItems="center" justifyContent="space-between">
                <Text>
                  <FormattedMessage id="jobs.failure.expandLogs" />
                </Text>
                <FlexContainer>
                  <Tooltip control={<Button variant="secondary" icon="eye" onClick={openLogsModal} />}>
                    <FormattedMessage id="jobs.failure.viewLogs" />
                  </Tooltip>
                  <Tooltip
                    control={
                      <DownloadButton
                        configType={configType}
                        id={jobId}
                        logLines={
                          areLogsFormatted(logs)
                            ? logs.logLines
                            : (areLogsStructured(logs) && logs.events.map(formatLogEvent)) || []
                        }
                      />
                    }
                  >
                    <FormattedMessage id="sources.downloadLogs" />
                  </Tooltip>
                </FlexContainer>
              </FlexContainer>
            </FlexItem>
          )}

          {/* Show failure details if available */}
          {hasFailureDetails && (
            <FlexItem>
              <DisclosureHeader
                toggle={toggleDetails}
                expanded={isDetailsExpanded}
                messageId="jobs.failure.expandDetails"
              />
              {isDetailsExpanded && (
                <Text as="div">
                  <FlexContainer direction="column" className={styles.details}>
                    {failureReason.internalMessage && (
                      <FlexItem>
                        <FormattedMessage id="jobs.failure.internalMessageLabel" /> {failureReason.internalMessage}
                      </FlexItem>
                    )}
                    {failureReason.failureOrigin && (
                      <FlexItem>
                        <FormattedMessage id="jobs.failure.originLabel" /> {failureReason.failureOrigin}
                      </FlexItem>
                    )}
                    {failureReason.failureType && (
                      <FlexItem>
                        <FormattedMessage id="jobs.failure.typeLabel" /> {failureReason.failureType}
                      </FlexItem>
                    )}
                  </FlexContainer>
                </Text>
              )}
            </FlexItem>
          )}

          {/* Show stacktrace if available */}
          {failureReason?.stacktrace && (
            <FlexItem>
              <DisclosureHeader
                toggle={toggleStacktrace}
                expanded={isStacktraceExpanded}
                messageId="jobs.failure.expandStacktrace"
              />
              {isStacktraceExpanded && <Logs follow logsArray={failureReason.stacktrace.split("\n")} />}
            </FlexItem>
          )}
        </FlexContainer>
      )}
    </Message>
  );
};
