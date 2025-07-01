import classNames from "classnames";
import React, { useState } from "react";

import { Icon, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./Message.module.scss";
import { Button, ButtonProps } from "../Button";
import { FlexContainer } from "../Flex";

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
  iconOverride?: keyof typeof ICON_MAPPING;
  textClassName?: string;
  isExpandable?: boolean;
  header?: React.ReactNode;
}

const ICON_MAPPING: Readonly<Record<MessageType, IconType>> = {
  warning: "statusWarning",
  error: "statusError",
  success: "statusSuccess",
  info: "infoFilled",
};

const STYLES_BY_TYPE: Readonly<Record<MessageType, string>> = {
  warning: styles.warning,
  error: styles.error,
  success: styles.success,
  info: styles.info,
};

export const MESSAGE_SEVERITY_LEVELS: Readonly<Record<MessageType, number>> = {
  error: 3,
  warning: 2,
  info: 1,
  success: 0,
};

export const isHigherSeverity = (messageTypeA: MessageType, messageTypeB: MessageType): boolean => {
  return MESSAGE_SEVERITY_LEVELS[messageTypeA] > MESSAGE_SEVERITY_LEVELS[messageTypeB];
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
  iconOverride,
  textClassName,
  isExpandable = false,
  header,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const handleToggleExpand = () => {
    setIsExpanded((isExpanded) => !isExpanded);
  };

  const isRenderingChildren = children && (!isExpandable || isExpanded);

  const mainMessage = (
    <>
      {!hideIcon && (
        <div className={classNames(styles.iconContainer)}>
          <Icon type={iconOverride ? ICON_MAPPING[iconOverride] : ICON_MAPPING[type]} className={styles.messageIcon} />
        </div>
      )}
      <div className={classNames(styles.textContainer, textClassName)}>
        {text && <span className={styles.text}>{text}</span>}
        {secondaryText && (
          <Text size="md" className={styles.secondaryText}>
            {secondaryText}
          </Text>
        )}
      </div>
      {(onAction || isExpandable || onClose) && (
        <FlexContainer direction="row" alignItems="flex-end" className={styles.alignRightColumn}>
          {onAction && (
            <Button
              type="button"
              {...actionBtnProps}
              variant="primaryDark"
              onClick={onAction}
              data-testid={testId ? `${testId}-button` : undefined}
            >
              {actionBtnText}
            </Button>
          )}
          {isExpandable && (
            <Button type="button" variant="clear" onClick={handleToggleExpand}>
              <Icon color="affordance" type={isExpanded ? "chevronDown" : "chevronRight"} size="lg" />
            </Button>
          )}
          {onClose && (
            <Button type="button" variant="clear" onClick={onClose} size="xs" icon="cross" iconColor="affordance" />
          )}
        </FlexContainer>
      )}
    </>
  );

  return (
    <FlexContainer
      direction="column"
      className={classNames(className, styles.messageContainer, STYLES_BY_TYPE[type])}
      data-testid={testId}
    >
      {header}
      <FlexContainer alignItems="flex-start" gap="xs">
        {mainMessage}
      </FlexContainer>
      {isRenderingChildren && (
        <div
          className={classNames(styles.childrenContainer, childrenClassName, STYLES_BY_TYPE[type])}
          data-testid={testId ? `${testId}-children` : undefined}
        >
          {children}
        </div>
      )}
    </FlexContainer>
  );
};
