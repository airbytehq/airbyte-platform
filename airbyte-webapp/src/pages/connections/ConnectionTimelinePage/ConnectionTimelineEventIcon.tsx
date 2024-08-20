import classNames from "classnames";

import { Icon, IconProps } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import styles from "./ConnectionTimelineEventIcon.module.scss";
export const ConnectionTimelineEventIcon: React.FC<{
  icon: IconProps["type"];
  statusIcon?: IconProps["type"];
  running?: boolean;
}> = ({ icon, statusIcon, running }) => {
  return (
    <div className={classNames(styles.connectionTimelineEventIcon)}>
      {statusIcon && (
        <div className={styles.connectionTimelineEventIcon__statusIndicator}>
          <Icon
            type={statusIcon}
            color={
              statusIcon === "statusSuccess"
                ? "success"
                : statusIcon === "statusWarning"
                ? "warning"
                : statusIcon === "statusCancelled"
                ? "disabled"
                : statusIcon === "statusError"
                ? "error"
                : undefined
            }
            size="sm"
            className={styles.connectionTimelineEventIcon__statusIcon}
          />
        </div>
      )}
      {running && !statusIcon && (
        <CircleLoader title="syncing" className={styles.connectionTimelineEventIcon__running} />
      )}
      <Icon type={icon} size="sm" color="disabled" className={styles.connectionTimelineEventIcon__icon} />
    </div>
  );
};
