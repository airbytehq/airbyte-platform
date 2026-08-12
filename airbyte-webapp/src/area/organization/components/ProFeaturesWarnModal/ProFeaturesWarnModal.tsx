import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { PlanAvailabilityBadges } from "cloud/area/billing/components/PlanAvailabilityBadges";
import { links } from "core/utils/links";

import proUpsellGraphic from "./pro-upsell-graphic.png";
import styles from "./ProFeaturesWarnModal.module.scss";

interface ProFeaturesWarnModalProps {
  onContinue?: () => void;
  featureId?: string;
}

const genericProFeatureMessageIds = [
  "proFeatures.modal.features.subHourSyncs",
  "proFeatures.modal.features.multipleWorkspaces",
  "proFeatures.modal.features.rbac",
  "proFeatures.modal.features.mappers",
  "proFeatures.modal.features.connectors",
];

export const ProFeaturesWarnModal: React.FC<ProFeaturesWarnModalProps> = ({ onContinue, featureId }) => {
  const isPlusOrProFeature = featureId === "sub-hourly-sync";
  const titleMessageId = isPlusOrProFeature
    ? "proFeatures.modal.titleUpgradePlusOrPro"
    : "proFeatures.modal.titleUpgrade";
  const featureMessageIds = isPlusOrProFeature
    ? ["proFeatures.modal.features.subHourSyncsPlusOrPro"]
    : genericProFeatureMessageIds;
  const bodyMessageId = isPlusOrProFeature
    ? "proFeatures.modal.upgradeMessagePlusOrPro"
    : "proFeatures.modal.upgradeMessage";

  return (
    <ModalBody className={styles.proFeaturesModal}>
      <FlexContainer direction="row" gap="none" className={styles.proFeaturesModal__layout}>
        <section className={styles.proFeaturesModal__content}>
          <FlexContainer direction="column" gap="lg">
            {isPlusOrProFeature ? (
              <PlanAvailabilityBadges plans={["plus", "pro"]} />
            ) : (
              <BrandingBadge product="cloudForTeams" />
            )}
            <Text size="xl" bold>
              <FormattedMessage id={titleMessageId} />
            </Text>
            <Box>
              <ul className={styles.proFeaturesModal__featuresList}>
                {featureMessageIds.map((messageId) => (
                  <li key={messageId}>
                    <FormattedMessage id={messageId} />
                  </li>
                ))}
              </ul>
            </Box>
            <Text size="lg" align="left">
              <FormattedMessage id={bodyMessageId} />
            </Text>
            <FlexContainer direction="row">
              <ExternalLink href={links.contactSales} opensInNewTab>
                <Button variant="primary">
                  <FormattedMessage id="proFeatures.modal.button.talkToSales" />
                </Button>
              </ExternalLink>
              <Button variant="secondary" onClick={onContinue}>
                <FormattedMessage id="proFeatures.modal.button.noThanks" />
              </Button>
            </FlexContainer>
          </FlexContainer>
        </section>
        <aside className={styles.proFeaturesModal__image}>
          <img src={proUpsellGraphic} alt="Pro upsell graphic" className={styles.proFeaturesModal__imageImg} />
        </aside>
      </FlexContainer>
    </ModalBody>
  );
};
