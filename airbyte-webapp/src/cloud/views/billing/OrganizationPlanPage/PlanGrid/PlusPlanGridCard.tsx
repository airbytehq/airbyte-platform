import React, { useState } from "react";
import { FormattedMessage, FormattedNumber } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ListBox, ListBoxControlButtonProps, Option } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import {
  CustomerPortalRequestBodyPlan,
  OrganizationSubscriptionInfoReadSelfServePlan,
} from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { PlanGridCard, PlanGridFeatureList } from "./PlanGridCard";
import styles from "./PlanGridCard.module.scss";

interface PlusPlanTier {
  credits: number;
  monthlyPrice: number;
  overageRate: number;
  plan: CustomerPortalRequestBodyPlan;
}

export const PLUS_PLAN_TIERS: PlusPlanTier[] = [
  { credits: 40, monthlyPrice: 189, overageRate: 5, plan: "plus_40" },
  { credits: 100, monthlyPrice: 449, overageRate: 5, plan: "plus_100" },
  { credits: 250, monthlyPrice: 999, overageRate: 4.5, plan: "plus_250" },
  { credits: 500, monthlyPrice: 1799, overageRate: 4.15, plan: "plus_500" },
  { credits: 1000, monthlyPrice: 3199, overageRate: 3.75, plan: "plus_1000" },
  { credits: 2000, monthlyPrice: 4999, overageRate: 2.5, plan: "plus_2000" },
];

/** Tier shown to orgs that are not on Plus before they pick one. */
const DEFAULT_TIER_CREDITS = 40;

/**
 * Orgs already on Plus cannot pick their own tier again, so they start on the next tier up,
 * or the next tier down when they are already on the top tier.
 */
const defaultTierFor = (currentTier: PlusPlanTier | undefined): PlusPlanTier => {
  if (!currentTier) {
    return PLUS_PLAN_TIERS.find((tier) => tier.credits === DEFAULT_TIER_CREDITS) ?? PLUS_PLAN_TIERS[0];
  }
  const index = PLUS_PLAN_TIERS.indexOf(currentTier);
  return PLUS_PLAN_TIERS[index + 1] ?? PLUS_PLAN_TIERS[index - 1];
};

const CreditsControlButtonContent: React.FC<ListBoxControlButtonProps<number>> = ({ selectedOption }) => (
  <Text as="span" size="lg">
    {selectedOption?.label}
  </Text>
);

type PlusCtaAction = "subscribe" | "upgrade" | "tierUpgrade" | "tierDowngrade";

const CTA_MESSAGE_IDS: Record<PlusCtaAction, string> = {
  subscribe: "plans.plus.get",
  upgrade: "plans.plus.upgrade",
  tierUpgrade: "plans.plus.tierUpgrade",
  tierDowngrade: "plans.plus.tierDowngrade",
};

const CONFIRMATION_MESSAGE_IDS: Record<
  Exclude<PlusCtaAction, "subscribe">,
  { title: string; text: string; submit: string; cancel: string }
> = {
  upgrade: {
    title: "plans.plus.upgrade.confirmTitle",
    text: "plans.plus.upgrade.confirmText",
    submit: "plans.plus.upgrade.confirmSubmit",
    cancel: "plans.plus.upgrade.confirmCancel",
  },
  tierUpgrade: {
    title: "plans.plus.tierUpgrade.confirmTitle",
    text: "plans.plus.tierUpgrade.confirmText",
    submit: "plans.plus.tierUpgrade.confirmSubmit",
    cancel: "plans.plus.tierUpgrade.confirmCancel",
  },
  tierDowngrade: {
    title: "plans.plus.tierDowngrade.confirmTitle",
    text: "plans.plus.tierDowngrade.confirmText",
    submit: "plans.plus.tierDowngrade.confirmSubmit",
    cancel: "plans.plus.tierDowngrade.confirmCancel",
  },
};

interface PlusPlanGridCardProps {
  disabled?: boolean;
  /** The org is on a paid self-serve plan other than Plus, so getting Plus is an upgrade. */
  isPaidPlan?: boolean;
  /** The org is currently on Plus. */
  isCurrentPlan?: boolean;
  /** The self-serve plan the org's subscription is on. Identifies the current Plus tier when known. */
  currentPlan?: OrganizationSubscriptionInfoReadSelfServePlan;
  cancellationDate?: string;
}

export const PlusPlanGridCard: React.FC<PlusPlanGridCardProps> = ({
  disabled = false,
  isPaidPlan = false,
  isCurrentPlan = false,
  currentPlan,
  cancellationDate,
}) => {
  const currentTier = isCurrentPlan ? PLUS_PLAN_TIERS.find((tier) => tier.plan === currentPlan) : undefined;
  const [selectedCredits, setSelectedCredits] = useState<number>();
  const chosenTier = PLUS_PLAN_TIERS.find((tier) => tier.credits === selectedCredits && tier !== currentTier);
  const selectedTier = chosenTier ?? defaultTierFor(currentTier);

  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("setup", selectedTier.plan);
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const creditOptions: Array<Option<number>> = PLUS_PLAN_TIERS.map((tier) => ({
    value: tier.credits,
    icon: "checkCircle",
    disabled: tier === currentTier,
    label: (
      <FormattedMessage
        id={tier === currentTier ? "planGrid.plus.creditsOption.current" : "planGrid.plus.creditsOption"}
        values={{
          credits: tier.credits,
          monthlyPrice: (
            <FormattedNumber value={tier.monthlyPrice} style="currency" currency="USD" maximumFractionDigits={0} />
          ),
        }}
      />
    ),
  }));

  const action: PlusCtaAction = currentTier
    ? selectedTier.credits > currentTier.credits
      ? "tierUpgrade"
      : "tierDowngrade"
    : isPaidPlan
    ? "upgrade"
    : "subscribe";

  const onClick = () => {
    if (action === "subscribe") {
      goToCustomerPortal();
      return;
    }

    const messageIds = CONFIRMATION_MESSAGE_IDS[action];
    const messageValues = { credits: <FormattedNumber value={selectedTier.credits} /> };
    openConfirmationModal({
      title: <FormattedMessage id={messageIds.title} values={messageValues} />,
      text: (
        <Text>
          <FormattedMessage id={messageIds.text} values={messageValues} />
        </Text>
      ),
      submitButtonText: messageIds.submit,
      cancelButtonText: messageIds.cancel,
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
        isCurrentPlan && !currentTier ? (
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
            <Button
              full
              isLoading={redirecting}
              disabled={disabled}
              variant={action === "tierDowngrade" ? "secondary" : "primary"}
              onClick={onClick}
            >
              <FormattedMessage id={CTA_MESSAGE_IDS[action]} />
            </Button>
          </FlexContainer>
        )
      }
    />
  );
};
