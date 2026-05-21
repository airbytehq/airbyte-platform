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

interface PlusPlanCardProps {
  disabled?: boolean;
  isPaidPlan?: boolean;
}

export const PlusPlanCard: React.FC<PlusPlanCardProps> = ({ disabled = false, isPaidPlan = false }) => {
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("setup", "plus");
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const ctaMessageId = isPaidPlan ? "plans.plus.upgrade" : "plans.plus.get";

  const onClick = () => {
    if (!isPaidPlan) {
      goToCustomerPortal();
      return;
    }

    openConfirmationModal({
      title: <FormattedMessage id="plans.plus.upgrade.confirmTitle" />,
      text: (
        <Text>
          <FormattedMessage id="plans.plus.upgrade.confirmText" />
        </Text>
      ),
      submitButtonText: "plans.plus.upgrade.confirmSubmit",
      cancelButtonText: "plans.plus.upgrade.confirmCancel",
      onSubmit: () => {
        closeConfirmationModal();
        goToCustomerPortal();
      },
    });
  };

  return (
    <PlanCard variant="purple">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Heading as="h3" size="md">
          <FormattedMessage id="plans.plus.title" />
        </Heading>
        <FlexContainer alignItems="center">
          <Text size="lg" bold>
            <FormattedMessage id="plans.plus.price" />
          </Text>
          <Button isLoading={redirecting} disabled={disabled} variant="primary" onClick={onClick}>
            <FormattedMessage id={ctaMessageId} />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <Text as="div" size="lg" className={styles.planCardText}>
        <p>
          <FormattedMessage id="plans.plus.description" />
        </p>
        <ul>
          <FormattedMessage
            id="plans.plus.features"
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
