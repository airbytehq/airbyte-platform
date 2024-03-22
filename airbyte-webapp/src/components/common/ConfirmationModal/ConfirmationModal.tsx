import isString from "lodash/isString";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Modal } from "components/ui/Modal";

import useLoadingState from "hooks/useLoadingState";

import styles from "./ConfirmationModal.module.scss";

export interface ConfirmationModalProps {
  title: string | React.ReactNode;
  text: string | React.ReactNode;
  textValues?: Record<string, string | number>;
  onCancel: () => void;
  onSubmit: () => void;
  cancelButtonText?: string;
  submitButtonText: string;
  submitButtonDataId?: string;
  additionalContent?: React.ReactNode;
  submitButtonVariant?: "danger" | "primary";
}

export const ConfirmationModal: React.FC<ConfirmationModalProps> = ({
  title,
  text,
  additionalContent,
  textValues,
  onCancel,
  onSubmit,
  submitButtonText,
  submitButtonDataId,
  cancelButtonText,
  submitButtonVariant = "danger",
}) => {
  const { isLoading, startAction } = useLoadingState();
  const onSubmitBtnClick = () => startAction({ action: async () => onSubmit() });

  return (
    <Modal
      onCancel={onCancel}
      title={isString(title) ? <FormattedMessage id={title} /> : title}
      testId="confirmationModal"
    >
      <div className={styles.content}>
        {isString(text) ? <FormattedMessage id={text} values={textValues} /> : text}
        {additionalContent}
        <div className={styles.buttonContent}>
          <Button
            className={styles.buttonWithMargin}
            onClick={onCancel}
            type="button"
            variant="secondary"
            disabled={isLoading}
          >
            <FormattedMessage id={cancelButtonText ?? "form.cancel"} />
          </Button>
          <Button
            variant={submitButtonVariant}
            onClick={onSubmitBtnClick}
            data-id={submitButtonDataId}
            isLoading={isLoading}
          >
            <FormattedMessage id={submitButtonText} />
          </Button>
        </div>
      </div>
    </Modal>
  );
};
