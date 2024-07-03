import classNames from "classnames";

import { Icon } from "components/ui/Icon";

import styles from "./ConnectionTimelineEventIcon.module.scss";
import { ConnectionTimelineEventType } from "./utils";
export const ConnectionTimelineEventIcon: React.FC<{ isLast: boolean; eventType: ConnectionTimelineEventType }> = ({
  isLast,
  eventType,
}) => {
  const isFailure = eventType.includes("failed");
  const isCancelled = eventType.includes("cancelled");
  const isSuccess = eventType.includes("succeeded");
  const isIncomplete = eventType.includes("incomplete");

  return (
    <div
      className={classNames(styles.connectionTimelineEventIcon__container, {
        [styles["connectionTimelineEventIcon__container--last"]]: isLast,
      })}
    >
      {(isFailure || isCancelled || isSuccess || isIncomplete) && (
        <div className={styles.connectionTimelineEventIcon__statusIndicator}>
          <Icon
            type={
              isFailure
                ? "statusError"
                : isCancelled
                ? "statusCancelled"
                : isIncomplete
                ? "statusWarning"
                : "statusSuccess"
            }
            color={isFailure ? "error" : isCancelled ? "disabled" : isIncomplete ? "warning" : "success"}
            size="sm"
            className={classNames(styles.connectionTimelineEventIcon__statusIcon)}
          />
        </div>
      )}
      <Icon type="sync" size="sm" color="disabled" className={styles.connectionTimelineEventIcon__icon} />
    </div>
  );
};
