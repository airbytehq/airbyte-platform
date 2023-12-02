import { Disclosure } from "@headlessui/react";
import classNames from "classnames";
import React from "react";

import Indicator from "components/Indicator";
import { Icon } from "components/ui/Icon";

import styles from "./Collapsible.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";

interface CollapsibleProps {
  className?: string;
  buttonClassName?: string;
  label: string;
  showErrorIndicator?: boolean;
  type?: "footer" | "inline" | "section";
  hideWhenEmpty?: boolean;
  "data-testid"?: string;
  initiallyOpen?: boolean;
  onClick?: (newOpenState: boolean) => void;
}

export const Collapsible: React.FC<React.PropsWithChildren<CollapsibleProps>> = ({
  className,
  buttonClassName,
  label,
  showErrorIndicator = false,
  type = "inline",
  hideWhenEmpty = false,
  children,
  "data-testid": dataTestId,
  initiallyOpen = false,
  onClick,
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
            className={classNames(buttonClassName, styles.button, { [styles.buttonSection]: type === "section" })}
            onClick={() => onClick?.(!open)}
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
                <Icon type="chevronRight" />
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
