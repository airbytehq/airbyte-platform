import { faChevronRight } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React from "react";
import { useToggle } from "react-use";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import styles from "./Section.module.scss";

export interface SectionProps {
  title?: React.ReactNode;
  flush?: boolean;
  className?: string;
  collapsible?: boolean;
  collapsedPreviewInfo?: React.ReactNode;
  collapsedInitially?: boolean;
  testId?: string;
  flexHeight?: boolean;
}

export const Section: React.FC<React.PropsWithChildren<SectionProps>> = ({
  title,
  flush,
  children,
  className,
  collapsible = false,
  collapsedPreviewInfo,
  collapsedInitially = false,
  testId,
  flexHeight,
}) => {
  const [isCollapsed, setIsCollapsed] = useToggle(collapsedInitially);

  if (collapsedInitially && !collapsible) {
    // console.warn("Section cannot be collapsed initially if it is not collapsible");
  }

  return (
    <Card className={classNames({ [styles.flexHeight]: flexHeight })}>
      <div
        className={classNames(styles.section, { [styles.flush]: flush, [styles.flexHeight]: flexHeight }, className)}
      >
        <div className={styles.header}>
          {title && (
            <Heading as="h2" size="sm">
              {title}
            </Heading>
          )}
          {collapsible && (
            <Button
              variant="clear"
              onClick={setIsCollapsed}
              data-testid={`${testId}-section-expand-arrow`}
              type="button"
            >
              <FontAwesomeIcon
                className={classNames(styles.arrow, { [styles.expanded]: !isCollapsed })}
                icon={faChevronRight}
              />
            </Button>
          )}
        </div>
        {collapsible && isCollapsed && collapsedPreviewInfo}
        {collapsible ? !isCollapsed && children : children}
      </div>
    </Card>
  );
};
