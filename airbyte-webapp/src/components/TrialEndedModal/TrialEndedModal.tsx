import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";
import { ModalContentProps } from "hooks/services/Modal/types";
import { useRedirectToCustomerPortal } from "packages/cloud/area/billing/utils/useRedirectToCustomerPortal";

import styles from "./TrialEndedModal.module.scss";

interface TrialEndedModalResult {
  action: "standard" | "pro";
}

export const TrialEndedModal: React.FC<ModalContentProps<TrialEndedModalResult>> = ({ onComplete }) => {
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("setup");
  const [isProRedirecting, setIsProRedirecting] = useState(false);

  const handleStandardPlanClick = async () => {
    await goToCustomerPortal();
    onComplete({ action: "standard" });
  };

  const handleProPlanClick = () => {
    setIsProRedirecting(true);
    window.open(links.contactSales, "_blank");
    onComplete({ action: "pro" });
  };

  return (
    <ModalBody className={styles.trialEndedModal}>
      <FlexContainer direction="column">
        <FlexContainer direction="column" gap="lg" alignItems="center">
          <Text size="xl" bold>
            <FormattedMessage id="trialEnded.modal.title" />
          </Text>
          <Text size="sm" align="center">
            <FormattedMessage id="trialEnded.modal.description" />
          </Text>
        </FlexContainer>

        <FlexContainer direction="row" gap="lg">
          {/* Standard Plan */}
          <Box className={styles.trialEndedModal__planCard}>
            <FlexContainer direction="column" gap="lg" justifyContent="space-between">
              <FlexContainer direction="column" gap="lg">
                <FlexContainer
                  direction="column"
                  gap="sm"
                  alignItems="center"
                  className={styles.trialEndedModal__planTitle}
                >
                  <Heading as="h2" size="lg" color="blue">
                    <FormattedMessage id="trialEnded.modal.standard.title" />
                  </Heading>
                  <Text size="sm" align="center">
                    <FormattedMessage id="trialEnded.modal.standard.subtitle" />
                  </Text>
                </FlexContainer>

                <FlexContainer direction="column" gap="md">
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.connectors" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.workspace" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.syncFreq" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.users" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.builder" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.standard.feature.support" />
                    </Text>
                  </FlexContainer>
                </FlexContainer>
              </FlexContainer>

              <FlexContainer direction="column" alignItems="center" gap="lg">
                <Text size="md" bold>
                  <FormattedMessage id="trialEnded.modal.standard.price" />
                </Text>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={handleStandardPlanClick}
                  isLoading={redirecting}
                  className={styles.trialEndedModal__button}
                >
                  <FormattedMessage id="trialEnded.modal.standard.button" />
                </Button>
              </FlexContainer>
            </FlexContainer>
          </Box>

          {/* Pro Plan */}
          <Box className={styles.trialEndedModal__planCard}>
            <FlexContainer direction="column" gap="lg" justifyContent="space-between">
              <FlexContainer direction="column" gap="lg">
                <FlexContainer
                  direction="column"
                  gap="sm"
                  alignItems="center"
                  className={styles.trialEndedModal__planTitle}
                >
                  <Heading as="h2" size="lg">
                    <FormattedMessage id="trialEnded.modal.pro.title" />
                  </Heading>
                  <Text size="sm" align="center">
                    <FormattedMessage id="trialEnded.modal.pro.subtitle" />
                  </Text>
                </FlexContainer>

                <FlexContainer direction="column" gap="md">
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm" color="blue">
                      <FormattedMessage id="trialEnded.modal.pro.feature.everything" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.workspaces" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.syncFreq" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.rbac" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.sso" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.audit" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.encryption" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.pro.feature.support" />
                    </Text>
                  </FlexContainer>
                </FlexContainer>
              </FlexContainer>

              <FlexContainer direction="column" alignItems="center">
                <Button
                  variant="primaryDark"
                  size="sm"
                  onClick={handleProPlanClick}
                  isLoading={isProRedirecting}
                  className={styles.trialEndedModal__button}
                >
                  <FormattedMessage id="trialEnded.modal.pro.button" />
                </Button>
              </FlexContainer>
            </FlexContainer>
          </Box>
        </FlexContainer>
      </FlexContainer>
    </ModalBody>
  );
};
