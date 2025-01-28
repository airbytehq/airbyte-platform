import classNames from "classnames";

import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";

import { StreamReadLogsItem, StreamReadLogsItemLevel } from "core/api/types/ConnectorBuilderClient";

import styles from "./LogsDisplay.module.scss";
import { TestWarning } from "../useStreamTestMetadata";

interface LogsDisplayProps {
  logs: StreamReadLogsItem[];
  error?: string;
  testWarnings?: TestWarning[];
}

export const LogsDisplay: React.FC<LogsDisplayProps> = ({ logs, error, testWarnings }) => {
  const finalLogs = [...(error ? [{ level: "ERROR" as const, message: error }] : []), ...logs];

  return (
    <FlexContainer className={styles.container} direction="column">
      {finalLogs.map((log, index) => (
        <Message
          key={`${index}_${log.message}`}
          type={getMessageType(log.level)}
          text={log.message}
          secondaryText={log.internal_message}
          isExpandable={!!log.stacktrace}
        >
          {log.stacktrace && <Pre className={styles.stacktrace}>{log.stacktrace}</Pre>}
        </Message>
      ))}
      {testWarnings &&
        testWarnings.map((testWarning, index) => (
          <Message
            key={`${index}_${testWarning.message}`}
            className={classNames({ [styles.secondaryWarning]: testWarning.priority === "secondary" })}
            type="warning"
            text={testWarning.message}
          />
        ))}
    </FlexContainer>
  );
};

const getMessageType = (logLevel: StreamReadLogsItemLevel) => {
  switch (logLevel) {
    case "ERROR":
    case "FATAL":
      return "error";
    case "WARN":
      return "warning";
    default:
      return "info";
  }
};
