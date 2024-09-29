import isString from "lodash/isString";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Modal } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import useLoadingState from "hooks/useLoadingState";

import styles from "./ConfirmationModal.module.scss";

export interface ConfirmationModalProps {
  title: string | React.ReactNode;
  text: string | React.ReactNode;
  textValues?: Record<string, string | number>;
  onCancel: () => void;
  onSubmit: () => void;
  cancelButtonText?: string;
  confirmationText?: string;
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
  confirmationText,
  submitButtonVariant = "danger",
}) => {
  const { isLoading, startAction } = useLoadingState();
  const onSubmitBtnClick = () => startAction({ action: async () => onSubmit() });
  const [confirmationValue, setConfirmationValue] = React.useState("");

  return (
    <Modal
      onCancel={onCancel}
      title={isString(title) ? <FormattedMessage id={title} /> : title}
      testId="confirmationModal"
    >
      <div className={styles.content}>
        <Text>{isString(text) ? <FormattedMessage id={text} values={textValues} /> : text}</Text>
        {additionalContent}
        {confirmationText && (
          <Box pt="lg">
            <FlexContainer direction="column">
              {/* eslint-disable-next-line jsx-a11y/label-has-associated-control -- eslint loses the input even though it has an "htmlFor" */}
              <label htmlFor="confirmation-text">
                <Text>
                  <FormattedMessage
                    id="modal.confirmationTextDescription"
                    values={{
                      confirmationText: (
                        <Text as="span" bold>
                          {confirmationText}
                        </Text>
                      ),
                    }}
                  />
                </Text>
              </label>
              <Box mt="md">
                <Input
                  id="confirmation-text"
                  placeholder={confirmationText}
                  onChange={(event) => setConfirmationValue(event.target.value)}
                  value={confirmationValue}
                />
              </Box>
            </FlexContainer>
          </Box>
        )}
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
            disabled={!!confirmationText && confirmationValue.trim() !== confirmationText.trim()}
            isLoading={isLoading}
          >
            <FormattedMessage id={submitButtonText} />
          </Button>
        </div>
      </div>
    </Modal>
  );
};
