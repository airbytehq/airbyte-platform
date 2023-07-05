import { faCheck, faExclamationTriangle, faMinus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React from "react";

import { CrossIcon } from "components/icons/CrossIcon";
import { MoonIcon } from "components/icons/MoonIcon";
import { PauseIcon } from "components/icons/PauseIcon";

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
  success: faCheck,
  warning: faExclamationTriangle,
  cancelled: faMinus,
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
              <PauseIcon title={title} />
            ) : status === "sleep" ? (
              <MoonIcon title={title} />
            ) : status === "error" ? (
              <CrossIcon title={title} />
            ) : status === "loading" ? (
              <CircleLoader title={title} />
            ) : (
              <FontAwesomeIcon icon={_iconByStatus[status]} title={title} />
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
