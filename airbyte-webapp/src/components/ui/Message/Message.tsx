import { faCheck, faExclamation, faTimes } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React from "react";

import { CrossIcon } from "components/icons/CrossIcon";
import { Text } from "components/ui/Text";

import styles from "./Message.module.scss";
import { Button } from "../Button";

export const enum MessageType {
  WARNING = "warning",
  SUCCESS = "success",
  ERROR = "error",
  INFO = "info",
}

export interface MessageProps {
  className?: string;
  childrenClassName?: string;
  text: string | React.ReactNode;
  secondaryText?: string | React.ReactNode;
  type?: MessageType;
  onAction?: () => void;
  actionBtnText?: string;
  onClose?: () => void;
  "data-testid"?: string;
}

const ICON_MAPPING = {
  [MessageType.WARNING]: faExclamation,
  [MessageType.ERROR]: faTimes,
  [MessageType.SUCCESS]: faCheck,
  [MessageType.INFO]: faExclamation,
};

const STYLES_BY_TYPE: Readonly<Record<MessageType, string>> = {
  [MessageType.WARNING]: styles.warning,
  [MessageType.ERROR]: styles.error,
  [MessageType.SUCCESS]: styles.success,
  [MessageType.INFO]: styles.info,
};

export const Message: React.FC<React.PropsWithChildren<MessageProps>> = ({
  type = MessageType.INFO,
  onAction,
  actionBtnText,
  onClose,
  text,
  secondaryText,
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
      <div className={classNames(styles.iconContainer)}>
        <FontAwesomeIcon icon={ICON_MAPPING[type]} className={styles.messageIcon} />
      </div>
      <div className={styles.textContainer}>
        {text && (
          <Text size="lg" className={styles.text}>
            {text}
          </Text>
        )}
        {secondaryText && (
          <Text size="md" className={styles.secondaryText}>
            {secondaryText}
          </Text>
        )}
      </div>
      {onAction && (
        <Button variant="dark" onClick={onAction}>
          {actionBtnText}
        </Button>
      )}
      {onClose && (
        <Button variant="clear" className={styles.closeButton} onClick={onClose} size="sm" icon={<CrossIcon />} />
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
