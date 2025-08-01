import classNames from "classnames";
import { Suspense, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useToggle } from "react-use";

import Logs from "components/Logs";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import {
  CleanedLogLines,
  formatLogEvent,
  formatLogEventTimestamp,
} from "area/connection/components/JobHistoryItem/useCleanLogs";
import { VirtualLogs } from "area/connection/components/JobHistoryItem/VirtualLogs";
import { jobHasFormattedLogs, jobHasStructuredLogs } from "area/connection/utils/jobs";
import { JobConfigType, SynchronousJobRead } from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { downloadFile } from "core/utils/file";
import { useModalService } from "hooks/services/Modal";

import styles from "./JobFailure.module.scss";

export interface JobFailureProps {
  job?: SynchronousJobRead | null;
  fallbackMessage?: React.ReactNode;
}

/**
 * Hook that transforms SynchronousJobRead logs into CleanedLogLines format
 * Similar to useCleanLogs but specifically for SynchronousJobRead
 */
const useCleanJobLogs = (job: SynchronousJobRead): CleanedLogLines => {
  return useMemo(() => {
    if (!job.logs) {
      return [];
    }

    if (jobHasFormattedLogs(job) && job.logs.logLines) {
      return job.logs.logLines.map((line, index) => ({
        lineNumber: index + 1,
        original: line,
        text: line,
      }));
    }

    if (jobHasStructuredLogs(job) && job.logs.events) {
      return job.logs.events.map((event, index) => ({
        lineNumber: index + 1,
        original: JSON.stringify(event),
        text: formatLogEvent(event),
        timestamp: formatLogEventTimestamp(event.timestamp),
        level: event.level,
      }));
    }

    return [];
  }, [job]);
};

const JobLogsModalContent = ({ job }: { job: SynchronousJobRead }) => {
  const logLines = useCleanJobLogs(job);
  return (
    <div className={styles.logsModal}>
      <VirtualLogs
        attemptId={1}
        logLines={logLines}
        hasFailure={!job.succeeded}
        showStructuredLogs={jobHasStructuredLogs(job)}
      />
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

export const JobFailure: React.FC<JobFailureProps> = ({ job, fallbackMessage }) => {
  const [isDetailsExpanded, toggleDetails] = useToggle(false);
  const [isStacktraceExpanded, toggleStacktrace] = useToggle(false);
  const { openModal } = useModalService();

  if (!job) {
    return <Message text={fallbackMessage || <FormattedMessage id="form.someError" />} type="error" />;
  }

  const failureReason = job.failureReason;

  const openLogsModal = () => {
    openModal({
      size: "full",
      title: <FormattedMessage id="jobHistory.logs.title" values={{ connectionName: `${job.configType} job` }} />,
      content: () => (
        <DefaultErrorBoundary>
          <Suspense fallback={<LoadingSpinner />}>
            <JobLogsModalContent job={job} />
          </Suspense>
        </DefaultErrorBoundary>
      ),
    });
  };

  const hasLogs =
    (jobHasFormattedLogs(job) && job.logs?.logLines && job.logs.logLines.length > 0) ||
    (jobHasStructuredLogs(job) && job.logs?.events && job.logs.events.length > 0);

  const hasFailureDetails =
    failureReason && (failureReason.internalMessage || failureReason.failureOrigin || failureReason.failureType);

  return (
    <Message
      type="error"
      text={<FormattedMessage id={getTitlePhraseIdForJobConfigType(job.configType)} />}
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
                        configType={job.configType}
                        id={job.id}
                        logLines={
                          jobHasFormattedLogs(job) && job.logs?.logLines
                            ? job.logs.logLines
                            : (jobHasStructuredLogs(job) && job.logs?.events?.map(formatLogEvent)) || []
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
