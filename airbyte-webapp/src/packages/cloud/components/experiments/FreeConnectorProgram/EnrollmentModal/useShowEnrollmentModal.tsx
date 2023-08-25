import { useIntl } from "react-intl";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useStripeCheckout } from "core/api/cloud";
import { useAuthService } from "core/services/auth";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { EnrollmentModalContent } from "./EnrollmentModal";

export const useShowEnrollmentModal = () => {
  const { openModal, closeModal } = useModalService();
  const { mutateAsync: createCheckout } = useStripeCheckout();
  const workspaceId = useCurrentWorkspaceId();
  const { emailVerified, sendEmailVerification } = useAuthService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  const verifyEmail = () =>
    sendEmailVerification?.().then(() => {
      registerNotification({
        id: "fcp/verify-email",
        text: formatMessage({ id: "freeConnectorProgram.enrollmentModal.validationEmailConfirmation" }),
        type: "info",
      });
    });

  return {
    showEnrollmentModal: () => {
      openModal({
        title: null,
        content: () => (
          <EnrollmentModalContent
            workspaceId={workspaceId}
            createCheckout={createCheckout}
            closeModal={closeModal}
            emailVerified={emailVerified}
            sendEmailVerification={verifyEmail}
          />
        ),
      });
    },
  };
};
