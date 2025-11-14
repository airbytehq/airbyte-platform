import { FormattedMessage } from "react-intl";

import { ConfirmationModal } from "components/ConfirmationModal";

import { useResetDomainVerification } from "core/api";
import { DomainVerificationResponse } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";

interface ResetDomainConfirmationModalProps {
  domain: DomainVerificationResponse;
  onClose: () => void;
}

export const ResetDomainConfirmationModal: React.FC<ResetDomainConfirmationModalProps> = ({ domain, onClose }) => {
  const { registerNotification } = useNotificationService();
  const { mutateAsync: resetDomain } = useResetDomainVerification();

  const handleReset = async () => {
    try {
      await resetDomain(domain.id);

      registerNotification({
        id: "domain-verification-reset",
        text: <FormattedMessage id="settings.organizationSettings.domainVerification.resetSuccess" />,
        type: "success",
      });

      onClose();
    } catch (error) {
      registerNotification({
        id: "domain-verification-reset-error",
        text: <FormattedMessage id="settings.organizationSettings.domainVerification.resetError" />,
        type: "error",
      });
    }
  };

  return (
    <ConfirmationModal
      title="settings.organizationSettings.domainVerification.resetDomain"
      text="settings.organizationSettings.domainVerification.resetConfirmation"
      textValues={{ domain: domain.domain }}
      onCancel={onClose}
      onSubmit={handleReset}
      submitButtonText="settings.organizationSettings.domainVerification.resetButton"
      submitButtonVariant="primary"
    />
  );
};
