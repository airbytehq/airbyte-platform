import { faCheck, faExclamation, faTimes, faInfo } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React from "react";

import { CrossIcon } from "components/icons/CrossIcon";
import { Text } from "components/ui/Text";

import styles from "./Message.module.scss";
import { Button, ButtonProps } from "../Button";

export type MessageType = "warning" | "success" | "error" | "info";

export interface MessageProps {
  className?: string;
  childrenClassName?: string;
  text: string | React.ReactNode;
  secondaryText?: string | React.ReactNode;
  type?: MessageType;
  onAction?: () => void;
  actionBtnText?: string | React.ReactNode;
  actionBtnProps?: Omit<ButtonProps, "variant" | "size">;
  onClose?: () => void;
  "data-testid"?: string;
  hideIcon?: boolean;
}

const ICON_MAPPING = {
  warning: faExclamation,
  error: faTimes,
  success: faCheck,
  info: faInfo,
};

const STYLES_BY_TYPE: Readonly<Record<MessageType, string>> = {
  warning: styles.warning,
  error: styles.error,
  success: styles.success,
  info: styles.info,
};

export const Message: React.FC<React.PropsWithChildren<MessageProps>> = ({
  type = "info",
  onAction,
  actionBtnText,
  actionBtnProps,
  onClose,
  text,
  secondaryText,
  hideIcon = false,
  "data-testid": testId,
  className,
  childrenClassName,
  children,
}) => {
  const mainMessage = (
    <div
      className={classNames(className, styles.messageContainer, STYLES_BY_TYPE[type], {
        [styles.messageContainerWithChildren]: Boolean(children),
      })}
      data-testid={testId}
    >
      {!hideIcon && (
        <div className={classNames(styles.iconContainer)}>
          <FontAwesomeIcon icon={ICON_MAPPING[type]} className={styles.messageIcon} />
        </div>
      )}
      <div className={styles.textContainer}>
        {text && <span className={styles.text}>{text}</span>}
        {secondaryText && (
          <Text size="md" className={styles.secondaryText}>
            {secondaryText}
          </Text>
        )}
      </div>
      {onAction && (
        <Button type="button" {...actionBtnProps} variant="dark" onClick={onAction} data-testid={`${testId}-button`}>
          {actionBtnText}
        </Button>
      )}
      {onClose && (
        <Button
          type="button"
          variant="clear"
          className={styles.closeButton}
          onClick={onClose}
          size="xs"
          icon={<CrossIcon />}
        />
      )}
    </div>
  );

  if (!children) {
    return mainMessage;
  }

  return (
    <div>
      {mainMessage}
      <div className={classNames(styles.childrenContainer, childrenClassName, STYLES_BY_TYPE[type])}>{children}</div>
    </div>
  );
};
