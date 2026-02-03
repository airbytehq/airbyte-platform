import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { ModalContentProps } from "core/services/Modal";

interface CloudSubscriptionSuccessModalProps extends ModalContentProps<void> {}

export const CloudSubscriptionSuccessModal: React.FC<CloudSubscriptionSuccessModalProps> = ({ onComplete }) => (
  <>
    <ModalBody>
      <FlexContainer direction="column" gap="xl">
        <Text size="md">
          <FormattedMessage id="settings.organization.billing.subscriptionSuccess.description" />
        </Text>
        <Text size="md">
          <FormattedMessage id="settings.organization.billing.subscriptionSuccess.proWarning" />
        </Text>
      </FlexContainer>
    </ModalBody>
    <ModalFooter>
      <Button variant="primary" onClick={() => onComplete()}>
        <FormattedMessage id="settings.organization.billing.subscriptionSuccess.gotItButton" />
      </Button>
    </ModalFooter>
  </>
);
