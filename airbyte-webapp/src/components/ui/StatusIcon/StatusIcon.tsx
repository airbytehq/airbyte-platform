import classNames from "classnames";
import React from "react";

import { Icon } from "components/ui/Icon";

import { CircleLoader } from "./CircleLoader";
import styles from "./StatusIcon.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";
import { Tooltip } from "../Tooltip";

export type StatusIconStatus = "sleep" | "inactive" | "success" | "warning" | "loading" | "error" | "cancelled";
type Size = "sm" | "md" | "lg";

interface StatusIconProps {
  className?: string;
  status?: StatusIconStatus;
  title?: string;
  size?: Size;
  value?: string | number;
}

const sizeStyles: Record<Size, string> = {
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
};

const colorStyles: Record<StatusIconStatus, string> = {
  sleep: styles.default,
  inactive: styles.default,
  success: styles.success,
  warning: styles.warning,
  error: styles.error,
  cancelled: styles.default,
  loading: styles.none,
};

const _iconByStatus = {
  success: "statusSuccess",
  warning: "statusWarning",
  cancelled: "statusCancelled",
} as const;

export const StatusIcon: React.FC<StatusIconProps> = ({ title, status = "error", size = "md", value }) => {
  return (
    <Tooltip
      disabled={!title}
      control={
        <FlexContainer
          className={classNames(styles.container, sizeStyles[size], colorStyles[status], {
            [styles.loading]: status === "loading",
            [styles.withValue]: value !== undefined,
          })}
          direction="row"
          justifyContent="center"
          alignItems="center"
          gap="none"
        >
          <FlexContainer className={classNames(styles.icon)} justifyContent="center" alignItems="center" gap="none">
            {status === "inactive" ? (
              <Icon type="statusInactive" title={title} className={classNames(styles.icon)} />
            ) : status === "sleep" ? (
              <Icon type="statusSleep" title={title} className={classNames(styles.icon)} />
            ) : status === "error" ? (
              <Icon type="statusError" title={title} className={classNames(styles.icon)} />
            ) : status === "loading" ? (
              <CircleLoader title={title} />
            ) : (
              <Icon type={_iconByStatus[status]} title={title} className={classNames(styles.icon)} />
            )}
          </FlexContainer>
          {value !== undefined && (
            <Text size="sm" className={styles.value}>
              {value}
            </Text>
          )}
        </FlexContainer>
      }
    >
      {title}
    </Tooltip>
  );
};
