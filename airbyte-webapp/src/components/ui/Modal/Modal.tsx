import { Dialog, DialogPanel } from "@headlessui/react";
import classNames from "classnames";
import React, { useState, useCallback, useRef, useEffect } from "react";
import { useIntl } from "react-intl";
import { useLocation } from "react-router-dom";

import styles from "./Modal.module.scss";
import { Box } from "../Box";
import { FlexContainer } from "../Flex";
import { Heading } from "../Heading";
import { Icon } from "../Icon";
import { Overlay } from "../Overlay";

export interface ModalProps {
  title?: string | React.ReactNode;
  /**
   * Function to call when the user press Escape, clicks on Backdrop clicks or X-button clicks.
   * Note: if openModal function was called with "preventCancel: true" then this function will not be called.
   */
  onCancel?: () => void;
  cardless?: boolean;
  size?: "sm" | "md" | "lg" | "xl" | "full";
  testId?: string;
  /**
   * If specified, the full content of the modal including header, body and footer is wrapped in this component (only a class name prop might be set on the component)
   */
  wrapIn?: React.FC<React.PropsWithChildren<{ className?: string }>>;
  allowNavigation?: boolean; // We block navigation by default, but occasionally want to allow redirects in the background
}

const cardStyleBySize = {
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
  full: styles.full,
};

const MODAL_ATTRIBUTE = "data-airbyte-modal";
const MODAL_SELECTOR = `[${MODAL_ATTRIBUTE}]`;

export const Modal: React.FC<React.PropsWithChildren<ModalProps>> = ({
  children,
  title,
  size,
  onCancel,
  cardless,
  testId,
  wrapIn,
  allowNavigation = false,
}) => {
  const [isOpen, setIsOpen] = useState(true);
  const { formatMessage } = useIntl();
  const location = useLocation();
  const originalLocation = useRef(location);

  const onModalCancel = useCallback(() => {
    if (onCancel) {
      setIsOpen(false);
      onCancel();
    }
  }, [onCancel]);

  useEffect(() => {
    if (location !== originalLocation.current && !allowNavigation) {
      setIsOpen(false);
      onModalCancel();
    }
  }, [allowNavigation, location, onModalCancel]);

  const Wrapper = wrapIn || "div";

  return (
    <Dialog open={isOpen} onClose={onModalCancel} data-testid={testId} className={styles.modalPageContainer}>
      <Overlay />
      <Wrapper
        className={classNames(styles.modalContainer, {
          [styles["modalContainer--noSidebarOffset"]]: size === "full",
        })}
        {...{ [MODAL_ATTRIBUTE]: true }}
      >
        <DialogPanel className={styles.modalPanel}>
          {cardless ? (
            children
          ) : (
            <div className={classNames(styles.card, size ? cardStyleBySize[size] : undefined)}>
              {title && (
                <FlexContainer alignItems="flex-start" justifyContent="space-between">
                  <Box p="xl">
                    <Heading as="h2" size="sm">
                      {title}
                    </Heading>
                  </Box>
                  {onCancel && (
                    <button
                      className={styles.card__closeButton}
                      onClick={onModalCancel}
                      aria-label={formatMessage({ id: "modal.closeButtonLabel" })}
                      data-testid="close-modal-button"
                    >
                      <Icon type="cross" />
                    </button>
                  )}
                </FlexContainer>
              )}
              {children}
            </div>
          )}
        </DialogPanel>
      </Wrapper>
    </Dialog>
  );
};

export const isElementInModal = (element: Element) => {
  return !!element.closest(MODAL_SELECTOR);
};

export const isAnyModalOpen = () => {
  return !!document.querySelector(MODAL_SELECTOR);
};
