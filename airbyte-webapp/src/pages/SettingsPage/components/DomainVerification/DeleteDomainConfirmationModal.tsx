import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ConfirmationModal } from "components/ui/ConfirmationModal";
import { Text } from "components/ui/Text";

import { useDeleteDomainVerification } from "core/api";
import { DomainVerificationResponse, SSOConfigRead } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";

interface DeleteDomainConfirmationModalProps {
  domain: DomainVerificationResponse;
  ssoConfig: SSOConfigRead | null;
  onClose: () => void;
}

export const DeleteDomainConfirmationModal: React.FC<DeleteDomainConfirmationModalProps> = ({
  domain,
  ssoConfig,
  onClose,
}) => {
  const { registerNotification } = useNotificationService();
  const { mutateAsync: deleteDomain } = useDeleteDomainVerification();

  const hasActiveSso = ssoConfig?.status === "active";
  const isVerifiedDomain = domain.status === "VERIFIED";
  const showSsoWarning = hasActiveSso && isVerifiedDomain;

  const handleDelete = async () => {
    try {
      await deleteDomain(domain.id);

      registerNotification({
        id: "domain-verification-deleted",
        text: <FormattedMessage id="settings.organizationSettings.domainVerification.deleted" />,
        type: "success",
      });

      onClose();
    } catch (error) {
      registerNotification({
        id: "domain-verification-delete-error",
        text: <FormattedMessage id="settings.organizationSettings.domainVerification.deleteError" />,
        type: "error",
      });
    }
  };

  return (
    <ConfirmationModal
      title="settings.organizationSettings.domainVerification.deleteDomain"
      text="settings.organizationSettings.domainVerification.deleteConfirmation"
      textValues={{ domain: domain.domain }}
      additionalContent={
        showSsoWarning && (
          <Box pt="lg">
            <Text color="red">
              <FormattedMessage id="settings.organizationSettings.domainVerification.deleteSsoWarning" />
            </Text>
          </Box>
        )
      }
      onCancel={onClose}
      onSubmit={handleDelete}
      submitButtonText="settings.organizationSettings.domainVerification.deleteButton"
      submitButtonVariant="danger"
    />
  );
};
