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
  variant?: "warning" | "upgrade";
  featureId?: string;
}

const genericProFeatureMessageIds = [
  "proFeatures.modal.features.subHourSyncs",
  "proFeatures.modal.features.multipleWorkspaces",
  "proFeatures.modal.features.rbac",
  "proFeatures.modal.features.mappers",
  "proFeatures.modal.features.connectors",
];

export const ProFeaturesWarnModal: React.FC<ProFeaturesWarnModalProps> = ({
  onContinue,
  variant = "warning",
  featureId,
}) => {
  const isUpgradeVariant = variant === "upgrade";
  const isPlusOrProFeature = featureId === "sub-hourly-sync";
  const titleMessageId = isPlusOrProFeature
    ? isUpgradeVariant
      ? "proFeatures.modal.titleUpgradePlusOrPro"
      : "proFeatures.modal.titlePlusOrPro"
    : isUpgradeVariant
    ? "proFeatures.modal.titleUpgrade"
    : "proFeatures.modal.title";
  const featuresTitleMessageId = isPlusOrProFeature
    ? "proFeatures.modal.featuresTitlePlusOrPro"
    : "proFeatures.modal.featuresTitle";
  const featureMessageIds = isPlusOrProFeature
    ? ["proFeatures.modal.features.subHourSyncsPlusOrPro"]
    : genericProFeatureMessageIds;
  const bodyMessageId = isPlusOrProFeature
    ? isUpgradeVariant
      ? "proFeatures.modal.upgradeMessagePlusOrPro"
      : "proFeatures.modal.warningMessagePlusOrPro"
    : isUpgradeVariant
    ? "proFeatures.modal.upgradeMessage"
    : "proFeatures.modal.warningMessage";

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
              {!isUpgradeVariant && (
                <Text size="lg" align="left">
                  <FormattedMessage id={featuresTitleMessageId} />
                </Text>
              )}
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
              {isUpgradeVariant ? (
                <>
                  <ExternalLink href={links.contactSales} opensInNewTab>
                    <Button variant="primary">
                      <FormattedMessage id="proFeatures.modal.button.talkToSales" />
                    </Button>
                  </ExternalLink>
                  <Button variant="secondary" onClick={onContinue}>
                    <FormattedMessage id="proFeatures.modal.button.noThanks" />
                  </Button>
                </>
              ) : (
                <>
                  <Button variant="primary" onClick={onContinue}>
                    <FormattedMessage id="proFeatures.modal.button.continue" />
                  </Button>
                  <ExternalLink href={links.contactSales} opensInNewTab>
                    <Button variant="secondary" icon="share" iconSize="sm" iconPosition="right">
                      <FormattedMessage id="proFeatures.modal.button.talkToSalesNow" />
                    </Button>
                  </ExternalLink>
                </>
              )}
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
