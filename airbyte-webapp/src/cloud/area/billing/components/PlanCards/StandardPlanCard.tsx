import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { PlanCard } from "./PlanCard";
import styles from "./PlanCard.module.scss";

export type StandardPlanCardMode = "subscribe" | "downgrade";

interface StandardPlanCardProps {
  disabled: boolean;
  mode?: StandardPlanCardMode;
}

export const StandardPlanCard: React.FC<StandardPlanCardProps> = ({ disabled, mode = "subscribe" }) => {
  const isDowngrade = mode === "downgrade";
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal(
    "setup",
    isDowngrade ? "standard" : undefined
  );
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const onClick = () => {
    if (!isDowngrade) {
      goToCustomerPortal();
      return;
    }

    openConfirmationModal({
      title: <FormattedMessage id="plans.standard.downgrade.confirmTitle" />,
      text: (
        <Text>
          <FormattedMessage id="plans.standard.downgrade.confirmText" />
        </Text>
      ),
      submitButtonText: "plans.standard.downgrade.confirmSubmit",
      cancelButtonText: "plans.standard.downgrade.confirmCancel",
      onSubmit: () => {
        closeConfirmationModal();
        goToCustomerPortal();
      },
    });
  };

  return (
    <PlanCard variant="primary">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Heading as="h3" size="md">
          <FormattedMessage id="plans.standard.title" />
        </Heading>
        <FlexContainer alignItems="center">
          <Text size="lg" bold>
            <FormattedMessage id="plans.standard.price" />
          </Text>
          <Button
            isLoading={redirecting}
            disabled={disabled}
            variant={isDowngrade ? "secondary" : "primary"}
            onClick={onClick}
          >
            <FormattedMessage id={isDowngrade ? "plans.standard.downgrade" : "plans.standard.subscribe"} />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <Text as="div" size="lg" className={styles.planCardText}>
        <p>
          <FormattedMessage id="plans.standard.description" />
        </p>
        <ul>
          <FormattedMessage
            id="plans.standard.features"
            values={{
              li: (node: React.ReactNode) => <li>{node}</li>,
              creditsLink: (node: React.ReactNode) => (
                <ExternalLink href={links.creditDescription} variant="primary">
                  {node}
                </ExternalLink>
              ),
            }}
          />
        </ul>
      </Text>
    </PlanCard>
  );
};
