import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import teamsUpsellGraphic from "./teams-upsell-graphic.png";
import styles from "./TeamsFeaturesWarnModal.module.scss";

interface TeamsFeaturesWarnModalProps {
  onContinue?: () => void;
}

export const TeamsFeaturesWarnModal: React.FC<TeamsFeaturesWarnModalProps> = ({ onContinue }) => (
  <ModalBody className={styles.teamsFeaturesModal}>
    <FlexContainer direction="row" gap="none" className={styles.teamsFeaturesModal__layout}>
      <section className={styles.teamsFeaturesModal__content}>
        <FlexContainer direction="column" gap="lg">
          <BrandingBadge product="cloudForTeams" />
          <Text size="xl" bold>
            <FormattedMessage id="teamsFeatures.modal.title" />
          </Text>
          <Box>
            <Text size="lg" align="left">
              <FormattedMessage id="teamsFeatures.modal.featuresTitle" />
            </Text>
            <ul className={styles.teamsFeaturesModal__featuresList}>
              <li>
                <FormattedMessage id="teamsFeatures.modal.features.subHourSyncs" />
              </li>
              <li>
                <FormattedMessage id="teamsFeatures.modal.features.multipleWorkspaces" />
              </li>
              <li>
                <FormattedMessage id="teamsFeatures.modal.features.sso" />
              </li>
              <li>
                <FormattedMessage id="teamsFeatures.modal.features.rbac" />
              </li>
              <li>
                <FormattedMessage id="teamsFeatures.modal.features.mappers" />
              </li>
            </ul>
          </Box>
          <Text size="lg" align="left">
            <FormattedMessage id="teamsFeatures.modal.warningMessage" />
          </Text>
          <FlexContainer direction="row">
            <Button variant="primary" onClick={onContinue}>
              <FormattedMessage id="teamsFeatures.modal.button.continue" />
            </Button>
          </FlexContainer>
        </FlexContainer>
      </section>
      <aside className={styles.teamsFeaturesModal__image}>
        <img src={teamsUpsellGraphic} alt="Teams upsell graphic" className={styles.teamsFeaturesModal__imageImg} />
      </aside>
    </FlexContainer>
  </ModalBody>
);
