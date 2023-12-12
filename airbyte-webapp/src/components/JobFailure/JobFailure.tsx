import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";
import { useToggle } from "react-use";

import Logs from "components/Logs";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { JobConfigType, SynchronousJobRead } from "core/api/types/AirbyteClient";
import { downloadFile } from "core/utils/file";

import styles from "./JobFailure.module.scss";

export interface JobFailureProps {
  job?: SynchronousJobRead | null;
  fallbackMessage?: React.ReactNode;
}

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
    <button
      className={styles.downloadButton}
      onClick={downloadFileWithLogs}
      type="button"
      title={formatMessage({
        id: "sources.downloadLogs",
      })}
    >
      <Icon type="download" />
    </button>
  );
};

export const JobFailure: React.FC<JobFailureProps> = ({ job, fallbackMessage }) => {
  const [isDetailsExpanded, toggleDetails] = useToggle(false);
  const [isStacktraceExpanded, toggleStacktrace] = useToggle(false);
  const [isLogsExpanded, toggleLogs] = useToggle(false);
  if (!job) {
    return <Message text={fallbackMessage || <FormattedMessage id="form.someError" />} type="error" />;
  }

  const failureReason = job.failureReason;
  return (
    <Message
      type="error"
      text={<FormattedMessage id={getTitlePhraseIdForJobConfigType(job.configType)} />}
      secondaryText={failureReason?.externalMessage || fallbackMessage}
    >
      {(failureReason || job.logs) && (
        <FlexContainer direction="column" gap="sm">
          {failureReason &&
            (failureReason.internalMessage || failureReason.failureOrigin || failureReason.failureType) && (
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
          {failureReason?.stacktrace && (
            <FlexItem>
              <DisclosureHeader
                toggle={toggleStacktrace}
                expanded={isStacktraceExpanded}
                messageId="jobs.failure.expandStacktrace"
              />
              {isStacktraceExpanded && <Logs logsArray={failureReason.stacktrace.split("\n")} />}
            </FlexItem>
          )}
          {job.logs?.logLines && job.logs.logLines.length > 0 && (
            <FlexItem>
              <DisclosureHeader
                toggle={toggleLogs}
                expanded={isLogsExpanded}
                messageId="jobs.failure.expandLogs"
                icon={<DownloadButton configType={job.configType} id={job.id} logLines={job.logs.logLines} />}
              />
              {isLogsExpanded && <Logs logsArray={job.logs.logLines} />}
            </FlexItem>
          )}
        </FlexContainer>
      )}
    </Message>
  );
};
