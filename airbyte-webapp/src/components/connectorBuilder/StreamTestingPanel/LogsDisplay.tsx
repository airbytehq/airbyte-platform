import classNames from "classnames";

import { FlexContainer } from "components/ui/Flex";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { StreamReadLogsItem } from "core/api/types/ConnectorBuilderClient";

import styles from "./LogsDisplay.module.scss";

interface LogsDisplayProps {
  logs: StreamReadLogsItem[];
  error?: string;
}

const Log: React.FC<{
  logMessage: StreamReadLogsItem;
}> = ({ logMessage }) => {
  return (
    <FlexContainer>
      <div
        className={classNames(styles.level, {
          [styles.error]: logMessage.level === "ERROR" || logMessage.level === "FATAL",
          [styles.warning]: logMessage.level === "WARN",
        })}
      >
        {logMessage.level}
      </div>
      <div className={styles.message}>
        <Pre>{logMessage.message}</Pre>
      </div>
    </FlexContainer>
  );
};

export const LogsDisplay: React.FC<LogsDisplayProps> = ({ logs, error }) => {
  return (
    <div className={styles.container}>
      {error !== undefined ? (
        <Text className={styles.error}>{error}</Text>
      ) : (
        <FlexContainer direction="column">
          {logs.map((log, index) => (
            <Log logMessage={log} key={index} />
          ))}
        </FlexContainer>
      )}
    </div>
  );
};
