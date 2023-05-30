import { faAngleRight } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Disclosure } from "@headlessui/react";
import classNames from "classnames";
import React from "react";

import Indicator from "components/Indicator";

import styles from "./Collapsible.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";

interface CollapsibleProps {
  className?: string;
  label: string;
  showErrorIndicator?: boolean;
  type?: "footer" | "inline" | "section";
  hideWhenEmpty?: boolean;
  "data-testid"?: string;
  initiallyOpen?: boolean;
}

export const Collapsible: React.FC<React.PropsWithChildren<CollapsibleProps>> = ({
  className,
  label,
  showErrorIndicator = false,
  type = "inline",
  hideWhenEmpty = false,
  children,
  "data-testid": dataTestId,
  initiallyOpen = false,
}) => {
  const childrenCount = React.Children.count(children);

  return childrenCount === 0 && hideWhenEmpty ? null : (
    <Disclosure defaultOpen={initiallyOpen}>
      {({ open }) => (
        <FlexContainer
          direction="column"
          alignItems="flex-start"
          className={classNames(className, styles.container, { [styles.footer]: type === "footer" })}
          gap="xl"
        >
          <Disclosure.Button
            data-testid={dataTestId}
            className={classNames(styles.button, { [styles.buttonSection]: type === "section" })}
          >
            <FlexContainer
              alignItems="center"
              gap="sm"
              className={classNames({
                [styles.buttonOpen]: open,
                [styles.buttonClosed]: !open && type !== "section",
                [styles.buttonClosedSection]: !open && type === "section",
              })}
            >
              <div
                className={classNames(styles.icon, {
                  [styles.iconOpen]: open,
                  [styles.iconClosed]: !open,
                })}
              >
                <FontAwesomeIcon icon={faAngleRight} />
              </div>
              <Text className={styles.label}>{label}</Text>
              {showErrorIndicator && <Indicator className={styles.errorIndicator} />}
            </FlexContainer>
          </Disclosure.Button>
          <Disclosure.Panel className={styles.body} unmount={false}>
            {children}
          </Disclosure.Panel>
        </FlexContainer>
      )}
    </Disclosure>
  );
};
