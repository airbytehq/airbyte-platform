import classNames from "classnames";
import React from "react";
import { useToggle } from "react-use";

import { Text } from "components/ui/Text";

import styles from "./Card.module.scss";
import { Box } from "../Box";
import { FlexContainer } from "../Flex";
import { Heading } from "../Heading";
import { Icon } from "../Icon";
import { InfoTooltip } from "../Tooltip";

export interface CardProps {
  /**
   * The title of the card
   */
  title?: string;
  helpText?: string;
  description?: React.ReactNode;
  /**
   * override card container styles
   */
  className?: string;
  /**
   * override card body styles
   */
  bodyClassName?: string;
  /**
   * If true, then card body will have no padding
   */
  noPadding?: boolean;
  /**
   * If true, then card title will have bottom border
   */
  titleWithBottomBorder?: boolean;
  /**
   * If true, then the card will be collapsible. Works with string title only
   */
  collapsible?: boolean;
  defaultCollapsedState?: boolean;
  collapsedPreviewInfo?: React.ReactNode;
  dataTestId?: string;
}

export const Card = React.forwardRef<HTMLDivElement, React.PropsWithChildren<CardProps>>(
  (
    {
      children,
      title,
      helpText,
      description,
      className,
      bodyClassName,
      noPadding = false,
      titleWithBottomBorder = false,
      collapsible,
      defaultCollapsedState = false,
      collapsedPreviewInfo,
      dataTestId,
      ...restProps
    },
    ref
  ) => {
    const [isCollapsed, toggleIsCollapsed] = useToggle(defaultCollapsedState);

    const headerTitle = (
      <FlexContainer
        direction="column"
        alignItems="flex-start"
        className={classNames(styles.cardHeader, {
          [styles.withBorderBottom]: titleWithBottomBorder,
          [styles.withPaddingBottom]: (isCollapsed && collapsible && !collapsedPreviewInfo) || !children,
        })}
      >
        {title && (
          <>
            <FlexContainer alignItems="center" gap="none">
              <Heading as="h5" size="sm">
                {title}
              </Heading>
              {description && (
                <InfoTooltip>
                  <Text className={styles.infoTooltip} size="sm">
                    {description}
                  </Text>
                </InfoTooltip>
              )}
            </FlexContainer>
            {collapsible && (
              <Icon
                type="chevronRight"
                size="lg"
                color="affordance"
                className={classNames(styles.icon, { [styles.expanded]: !isCollapsed })}
              />
            )}
          </>
        )}
        {helpText && (
          <Text className={styles.helpText} size="sm">
            {helpText}
          </Text>
        )}
      </FlexContainer>
    );

    return (
      <div className={classNames(className, styles.container)} data-testid={dataTestId} {...restProps} ref={ref}>
        {title && !collapsible ? headerTitle : null}
        {/* if collapsible and(if) preview info is provided */}
        {collapsible && (
          <button
            type="button"
            className={styles.headerBtn}
            onClick={toggleIsCollapsed}
            {...(dataTestId && { "data-testid": `${dataTestId}-card-expand-arrow` })}
          >
            {headerTitle}
            {isCollapsed && collapsedPreviewInfo && <Box p="xl">{collapsedPreviewInfo}</Box>}
          </button>
        )}
        <div
          className={classNames(
            styles.cardBody,
            {
              [styles.noPadding]: noPadding || (collapsible && isCollapsed),
            },
            bodyClassName
          )}
        >
          {collapsible ? !isCollapsed && children : children}
        </div>
      </div>
    );
  }
);
Card.displayName = "Card";
