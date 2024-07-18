import classNames from "classnames";

import { Icon, IconProps } from "components/ui/Icon";

import styles from "./ConnectionTimelineEventIcon.module.scss";
export const ConnectionTimelineEventIcon: React.FC<{
  isLast: boolean;
  icon: IconProps["type"];
  statusIcon?: IconProps["type"];
}> = ({ isLast, icon, statusIcon }) => {
  return (
    <div
      className={classNames(styles.connectionTimelineEventIcon__container, {
        [styles["connectionTimelineEventIcon__container--last"]]: isLast,
      })}
    >
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
            className={classNames(styles.connectionTimelineEventIcon__statusIcon)}
          />
        </div>
      )}
      <Icon type={icon} size="sm" color="disabled" className={styles.connectionTimelineEventIcon__icon} />
    </div>
  );
};
