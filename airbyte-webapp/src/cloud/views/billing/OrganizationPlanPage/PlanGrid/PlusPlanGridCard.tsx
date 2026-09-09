import React, { useState } from "react";
import { FormattedMessage, FormattedNumber } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ListBox, ListBoxControlButtonProps, Option } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { PlanGridCard, PlanGridFeatureList } from "./PlanGridCard";
import styles from "./PlanGridCard.module.scss";

interface PlusPlanTier {
  credits: number;
  monthlyPrice: number;
  overageRate: number;
}

export const PLUS_PLAN_TIERS: PlusPlanTier[] = [
  { credits: 100, monthlyPrice: 500, overageRate: 5 },
  { credits: 250, monthlyPrice: 1000, overageRate: 4 },
  { credits: 500, monthlyPrice: 1800, overageRate: 3.6 },
  { credits: 1000, monthlyPrice: 3000, overageRate: 3 },
];

const creditOptions: Array<Option<number>> = PLUS_PLAN_TIERS.map((tier) => ({
  value: tier.credits,
  icon: "checkCircle",
  label: <FormattedMessage id="planGrid.plus.creditsOption" values={{ credits: tier.credits }} />,
}));

const CreditsControlButtonContent: React.FC<ListBoxControlButtonProps<number>> = ({ selectedOption }) => (
  <Text as="span" size="lg">
    {selectedOption?.label}
  </Text>
);

interface PlusPlanGridCardProps {
  disabled?: boolean;
  isPaidPlan?: boolean;
  isCurrentPlan?: boolean;
  cancellationDate?: string;
}

export const PlusPlanGridCard: React.FC<PlusPlanGridCardProps> = ({
  disabled = false,
  isPaidPlan = false,
  isCurrentPlan = false,
  cancellationDate,
}) => {
  const [selectedCredits, setSelectedCredits] = useState(PLUS_PLAN_TIERS[0].credits);
  const selectedTier = PLUS_PLAN_TIERS.find((tier) => tier.credits === selectedCredits) ?? PLUS_PLAN_TIERS[0];

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
    <PlanGridCard
      data-testid="plus-plan-card"
      title={<FormattedMessage id="plans.plus.title" />}
      price={
        <FormattedNumber value={selectedTier.monthlyPrice} style="currency" currency="USD" maximumFractionDigits={0} />
      }
      pricePeriod={<FormattedMessage id="planGrid.perMonth" />}
      description={<FormattedMessage id="planGrid.plus.description" />}
      features={
        <PlanGridFeatureList
          messageId="planGrid.plus.features"
          values={{
            credits: <FormattedNumber value={selectedTier.credits} />,
            overageRate: (
              <FormattedNumber
                value={selectedTier.overageRate}
                style="currency"
                currency="USD"
                minimumFractionDigits={Number.isInteger(selectedTier.overageRate) ? 0 : 2}
                maximumFractionDigits={2}
              />
            ),
            creditsLink: (node: React.ReactNode) => (
              <ExternalLink href={links.creditDescription} variant="primary">
                {node}
              </ExternalLink>
            ),
          }}
        />
      }
      isCurrentPlan={isCurrentPlan}
      cancellationDate={cancellationDate}
      cta={
        isCurrentPlan ? (
          <Button full variant="secondary" disabled className={styles.planGridCard__currentPlan}>
            <FormattedMessage id="planGrid.currentPlan" />
          </Button>
        ) : (
          <FlexContainer direction="column" gap="md">
            <ListBox<number>
              options={creditOptions}
              selectedValue={selectedTier.credits}
              onSelect={setSelectedCredits}
              controlButtonContent={CreditsControlButtonContent}
              adaptiveWidth
              isDisabled={disabled}
              data-testid="plus-plan-credits"
            />
            <Button full isLoading={redirecting} disabled={disabled} variant="primary" onClick={onClick}>
              <FormattedMessage id={ctaMessageId} />
            </Button>
          </FlexContainer>
        )
      }
    />
  );
};
