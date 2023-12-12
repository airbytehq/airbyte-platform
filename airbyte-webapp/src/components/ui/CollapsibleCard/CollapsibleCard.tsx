import classNames from "classnames";
import React from "react";
import { useToggle } from "react-use";

import { FlexContainer } from "components/ui//Flex";
import { Icon } from "components/ui//Icon";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import styles from "./CollapsibleCard.module.scss";

export interface CollapsibleCardProps {
  title: React.ReactNode;
  children: React.ReactNode;
  collapsible?: boolean;
  defaultCollapsedState?: boolean;
  collapsedPreviewInfo?: React.ReactNode;
  testId?: string;
  className?: string;
}

export const CollapsibleCard: React.FC<CollapsibleCardProps> = ({
  title,
  children,
  collapsible = false,
  collapsedPreviewInfo,
  defaultCollapsedState = false,
  testId,
}) => {
  const [isCollapsed, toggleIsCollapsed] = useToggle(defaultCollapsedState);

  if (defaultCollapsedState && !collapsible) {
    console.warn("Card cannot be collapsed by default if it is not collapsible");
  }

  const headerContainer = (
    <FlexContainer justifyContent="space-between" alignItems="center" data-testid={`${testId}-card-expand-arrow`}>
      {title && (
        <Heading as="h2" size="sm">
          {title}
        </Heading>
      )}
      {collapsible && (
        <Icon
          type="chevronRight"
          size="lg"
          color="affordance"
          className={classNames(styles.icon, { [styles.expanded]: !isCollapsed })}
        />
      )}
    </FlexContainer>
  );

  return (
    <Card className={classNames(styles.container, { [styles.collapsed]: collapsible && isCollapsed })}>
      {collapsible ? (
        <button type="button" className={styles.headerBtn} onClick={toggleIsCollapsed}>
          {headerContainer}
          {isCollapsed && collapsedPreviewInfo && <Box mt="lg">{collapsedPreviewInfo}</Box>}
        </button>
      ) : (
        headerContainer
      )}
      {collapsible ? !isCollapsed && children : children}
    </Card>
  );
};
