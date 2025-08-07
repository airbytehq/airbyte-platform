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
  action: "cloud" | "teams";
}

export const TrialEndedModal: React.FC<ModalContentProps<TrialEndedModalResult>> = ({ onComplete }) => {
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("setup");
  const [isTeamsRedirecting, setIsTeamsRedirecting] = useState(false);

  const handleCloudPlanClick = async () => {
    await goToCustomerPortal();
    onComplete({ action: "cloud" });
  };

  const handleTeamsPlanClick = () => {
    setIsTeamsRedirecting(true);
    window.open(links.contactSales, "_blank");
    onComplete({ action: "teams" });
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
          {/* Cloud Plan */}
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
                    <FormattedMessage id="trialEnded.modal.cloud.title" />
                  </Heading>
                  <Text size="sm" align="center">
                    <FormattedMessage id="trialEnded.modal.cloud.subtitle" />
                  </Text>
                </FlexContainer>

                <FlexContainer direction="column" gap="md">
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.connectors" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.workspace" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.syncFreq" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.users" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.builder" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.cloud.feature.support" />
                    </Text>
                  </FlexContainer>
                </FlexContainer>
              </FlexContainer>

              <FlexContainer direction="column" alignItems="center" gap="lg">
                <Text size="md" bold>
                  <FormattedMessage id="trialEnded.modal.cloud.price" />
                </Text>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={handleCloudPlanClick}
                  isLoading={redirecting}
                  className={styles.trialEndedModal__button}
                >
                  <FormattedMessage id="trialEnded.modal.cloud.button" />
                </Button>
              </FlexContainer>
            </FlexContainer>
          </Box>

          {/* Teams Plan */}
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
                    <FormattedMessage id="trialEnded.modal.teams.title" />
                  </Heading>
                  <Text size="sm" align="center">
                    <FormattedMessage id="trialEnded.modal.teams.subtitle" />
                  </Text>
                </FlexContainer>

                <FlexContainer direction="column" gap="md">
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm" color="blue">
                      <FormattedMessage id="trialEnded.modal.teams.feature.everything" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.workspaces" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.syncFreq" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.rbac" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.sso" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.audit" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.encryption" />
                    </Text>
                  </FlexContainer>
                  <FlexContainer direction="row" gap="sm" alignItems="center">
                    <Icon type="check" size="sm" color="action" />
                    <Text size="sm">
                      <FormattedMessage id="trialEnded.modal.teams.feature.support" />
                    </Text>
                  </FlexContainer>
                </FlexContainer>
              </FlexContainer>

              <FlexContainer direction="column" alignItems="center">
                <Button
                  variant="primaryDark"
                  size="sm"
                  onClick={handleTeamsPlanClick}
                  isLoading={isTeamsRedirecting}
                  className={styles.trialEndedModal__button}
                >
                  <FormattedMessage id="trialEnded.modal.teams.button" />
                </Button>
              </FlexContainer>
            </FlexContainer>
          </Box>
        </FlexContainer>
      </FlexContainer>
    </ModalBody>
  );
};
