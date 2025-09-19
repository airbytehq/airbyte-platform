import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import proUpsellGraphic from "./pro-upsell-graphic.png";
import styles from "./ProFeaturesWarnModal.module.scss";

interface ProFeaturesWarnModalProps {
  onContinue?: () => void;
}

export const ProFeaturesWarnModal: React.FC<ProFeaturesWarnModalProps> = ({ onContinue }) => (
  <ModalBody className={styles.proFeaturesModal}>
    <FlexContainer direction="row" gap="none" className={styles.proFeaturesModal__layout}>
      <section className={styles.proFeaturesModal__content}>
        <FlexContainer direction="column" gap="lg">
          <BrandingBadge product="cloudForTeams" />
          <Text size="xl" bold>
            <FormattedMessage id="proFeatures.modal.title" />
          </Text>
          <Box>
            <Text size="lg" align="left">
              <FormattedMessage id="proFeatures.modal.featuresTitle" />
            </Text>
            <ul className={styles.proFeaturesModal__featuresList}>
              <li>
                <FormattedMessage id="proFeatures.modal.features.subHourSyncs" />
              </li>
              <li>
                <FormattedMessage id="proFeatures.modal.features.multipleWorkspaces" />
              </li>
              <li>
                <FormattedMessage id="proFeatures.modal.features.sso" />
              </li>
              <li>
                <FormattedMessage id="proFeatures.modal.features.rbac" />
              </li>
              <li>
                <FormattedMessage id="proFeatures.modal.features.mappers" />
              </li>
              <li>
                <FormattedMessage id="proFeatures.modal.features.connectors" />
              </li>
            </ul>
          </Box>
          <Text size="lg" align="left">
            <FormattedMessage id="proFeatures.modal.warningMessage" />
          </Text>
          <FlexContainer direction="row">
            <Button variant="primary" onClick={onContinue}>
              <FormattedMessage id="proFeatures.modal.button.continue" />
            </Button>
            <ExternalLink href={links.contactSales} opensInNewTab>
              <Button variant="secondary" icon="share" iconSize="sm" iconPosition="right">
                <FormattedMessage id="proFeatures.modal.button.talkToSalesNow" />
              </Button>
            </ExternalLink>
          </FlexContainer>
        </FlexContainer>
      </section>
      <aside className={styles.proFeaturesModal__image}>
        <img src={proUpsellGraphic} alt="Pro upsell graphic" className={styles.proFeaturesModal__imageImg} />
      </aside>
    </FlexContainer>
  </ModalBody>
);
