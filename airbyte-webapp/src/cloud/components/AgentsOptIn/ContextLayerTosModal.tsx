import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useNotificationService } from "core/services/Notification";

import styles from "./ContextLayerTosModal.module.scss";

interface ContextLayerTosModalProps {
  onCancel: () => void;
  onAccept: () => Promise<void>;
  onComplete: () => void;
}

export const ContextLayerTosModal: React.FC<ContextLayerTosModalProps> = ({ onCancel, onAccept, onComplete }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const [termsAccepted, setTermsAccepted] = useState(false);
  const [chargesAccepted, setChargesAccepted] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleAccept = async () => {
    setIsSubmitting(true);
    try {
      await onAccept();
      onComplete();
    } catch {
      registerNotification({
        id: "context-layer-enrollment-error",
        text: formatMessage({ id: "cloud.contextLayer.terms.enrollError" }),
        type: "error",
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <>
      <ModalBody>
        <Text size="sm" className={styles.subtitle}>
          <FormattedMessage id="cloud.contextLayer.terms.subtitle" />
        </Text>
        <div className={styles.terms}>
          <Text size="sm" bold>
            <FormattedMessage id="cloud.contextLayer.terms.intro" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.dataProcessing" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.privacyModeling" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.complianceResidency" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.liability" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.serviceLevel" />
          </Text>
          <Text size="sm">
            <FormattedMessage id="cloud.contextLayer.terms.intellectualProperty" />
          </Text>
        </div>
        <div className={styles.checkboxes}>
          <div className={styles.checkboxRow}>
            <CheckBox
              checkboxSize="sm"
              checked={termsAccepted}
              onChange={(event) => setTermsAccepted(event.target.checked)}
              aria-label={formatMessage({ id: "cloud.contextLayer.terms.acceptTerms" })}
            />
            <Text as="span" size="sm">
              <FormattedMessage id="cloud.contextLayer.terms.acceptTerms" />
            </Text>
          </div>
          <div className={styles.checkboxRow}>
            <CheckBox
              checkboxSize="sm"
              checked={chargesAccepted}
              onChange={(event) => setChargesAccepted(event.target.checked)}
              aria-label={formatMessage({ id: "cloud.contextLayer.terms.acceptCharges" })}
            />
            <Text as="span" size="sm">
              <FormattedMessage id="cloud.contextLayer.terms.acceptCharges" />
            </Text>
          </div>
        </div>
      </ModalBody>
      <ModalFooter>
        <div className={styles.footer}>
          <Text size="xs" color="grey">
            <FormattedMessage id="cloud.contextLayer.terms.footnote" />
          </Text>
          <div className={styles.buttons}>
            <Button type="button" variant="secondary" onClick={onCancel}>
              <FormattedMessage id="cloud.contextLayer.terms.cancel" />
            </Button>
            <Button
              type="button"
              variant="primaryDark"
              disabled={!termsAccepted || !chargesAccepted}
              isLoading={isSubmitting}
              onClick={handleAccept}
            >
              <FormattedMessage id="cloud.contextLayer.terms.accept" />
            </Button>
          </div>
        </div>
      </ModalFooter>
    </>
  );
};
