import classNames from "classnames";
import { FormattedMessage } from "react-intl";
import { LazyLog } from "react-lazylog";

import styles from "./Logs.module.scss";

interface LogsProps {
  logsArray?: string[];
  maxRows?: number;
}

const ROW_HEIGHT = 19;

function trimLogs(logs: string[]) {
  const trimmedLogs = [...logs];
  while (trimmedLogs.length > 0 && trimmedLogs[trimmedLogs.length - 1].trim() === "") {
    trimmedLogs.pop();
  }
  return trimmedLogs;
}

const Logs: React.FC<LogsProps> = ({ logsArray, maxRows = 21 }) => {
  const trimmedLogs = trimLogs(logsArray || []);
  const logsJoin = trimmedLogs.length ? trimmedLogs.join("\n") : "No logs available";

  return (
    <div
      className={classNames(styles.container, { [styles.empty]: !logsArray })}
      style={
        trimmedLogs.length
          ? {
              height: Math.min(maxRows, trimmedLogs.length) * ROW_HEIGHT,
            }
          : undefined
      }
    >
      {logsArray ? (
        <LazyLog
          rowHeight={ROW_HEIGHT}
          text={logsJoin}
          lineClassName={styles.logLine}
          highlightLineClassName={styles.highlightLogLine}
          selectableLines
          follow
          style={{ background: "transparent" }}
          scrollToLine={undefined}
          highlight={[]}
        />
      ) : (
        <FormattedMessage id="sources.emptyLogs" />
      )}
    </div>
  );
};

export default Logs;
