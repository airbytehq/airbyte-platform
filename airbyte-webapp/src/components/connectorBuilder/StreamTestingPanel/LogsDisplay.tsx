import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { NumberBadge } from "components/ui/NumberBadge";
import { Text } from "components/ui/Text";

import { StreamReadLogsItem } from "core/request/ConnectorBuilderClient";

import styles from "./LogsDisplay.module.scss";

interface LogsDisplayProps {
  logs: StreamReadLogsItem[];
  error?: string;
  onTitleClick: () => void;
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
        <pre>{logMessage.message}</pre>
      </div>
    </FlexContainer>
  );
};

export const LogsDisplay: React.FC<LogsDisplayProps> = ({ logs, error, onTitleClick }) => {
  return (
    <div className={styles.container}>
      <button className={styles.header} onClick={onTitleClick}>
        <Text size="sm" bold>
          <FormattedMessage id="connectorBuilder.connectorLogs" />
        </Text>
        {error !== undefined && <NumberBadge value={1} color="red" />}
      </button>
      <div className={styles.logsDisplay}>
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
    </div>
  );
};
