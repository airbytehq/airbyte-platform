import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import classNames from "classnames";
import React from "react";

import Indicator from "components/Indicator";
import { Icon } from "components/ui/Icon";

import styles from "./Collapsible.module.scss";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";
import { InfoTooltip } from "../Tooltip";

interface CollapsibleProps {
  className?: string;
  buttonClassName?: string;
  label: string;
  showErrorIndicator?: boolean;
  type?: "footer" | "inline" | "section";
  hideWhenEmpty?: boolean;
  "data-testid"?: string;
  initiallyOpen?: boolean;
  noBodyPadding?: boolean;
  onClick?: (newOpenState: boolean) => void;
  infoTooltipContent?: React.ReactNode;
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
  noBodyPadding = false,
  onClick,
  infoTooltipContent,
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
          <DisclosureButton
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
              <FlexContainer direction="row" gap="none" alignItems="center" className={styles.labelContainer}>
                <Text className={styles.label}>{label}</Text>
                {infoTooltipContent && <InfoTooltip placement="top-start">{infoTooltipContent}</InfoTooltip>}
              </FlexContainer>
              {showErrorIndicator && <Indicator className={styles.errorIndicator} />}
            </FlexContainer>
          </DisclosureButton>
          <DisclosurePanel
            className={classNames(styles.body, { [styles["body--noPadding"]]: noBodyPadding })}
            unmount={false}
          >
            {children}
          </DisclosurePanel>
        </FlexContainer>
      )}
    </Disclosure>
  );
};
