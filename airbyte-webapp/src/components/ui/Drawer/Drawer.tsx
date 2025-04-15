import { Transition, TransitionChild } from "@headlessui/react";
import React, { Fragment } from "react";
import { useIntl } from "react-intl";

import styles from "./Drawer.module.scss";
import { Button } from "../Button";
import { FlexContainer } from "../Flex";
import { Icon } from "../Icon";

interface DrawerProps {
  title?: React.ReactNode;
  children: React.ReactNode;
  isOpen: boolean;
  onClose: () => void;
  afterClose?: () => void;
}

const ARIA_LABELLED_BY = "drawer-title";

export const Drawer: React.FC<DrawerProps> = ({ title, children, isOpen, onClose, afterClose }) => {
  const { formatMessage } = useIntl();

  return (
    <Transition show={isOpen} as={Fragment} afterLeave={afterClose}>
      <div className={styles.drawer__container} role="dialog" aria-modal="false" aria-labelledby={ARIA_LABELLED_BY}>
        <TransitionChild
          as={Fragment}
          enter={styles["drawer--transitioning"]}
          enterFrom={styles["drawer--enterFrom"]}
          enterTo={styles["drawer--enterTo"]}
          leave={styles["drawer--transitioning"]}
          leaveFrom={styles["drawer--leaveFrom"]}
          leaveTo={styles["drawer--leaveTo"]}
        >
          <div className={styles.drawer__content}>
            <FlexContainer direction="column" gap="sm">
              <FlexContainer
                direction="row"
                alignItems="center"
                justifyContent={!!title ? "space-between" : "flex-end"}
                className={styles.drawer__header}
                id={ARIA_LABELLED_BY}
              >
                {title}

                <Button
                  type="button"
                  variant="clear"
                  onClick={onClose}
                  aria-label={formatMessage({ id: "modal.closeButtonLabel" })}
                >
                  <Icon type="cross" size="xl" />
                </Button>
              </FlexContainer>
              <div className={styles.drawer__childContainer}>{children}</div>
            </FlexContainer>
          </div>
        </TransitionChild>
      </div>
    </Transition>
  );
};
