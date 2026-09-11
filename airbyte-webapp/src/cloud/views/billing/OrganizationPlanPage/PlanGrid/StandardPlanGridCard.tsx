import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { StandardDowngradeConsequences } from "cloud/area/billing/components/PlanCards";
import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { PlanGridCard, PlanGridFeatureList } from "./PlanGridCard";
import styles from "./PlanGridCard.module.scss";

export type StandardPlanGridCardMode = "subscribe" | "downgrade";

interface StandardPlanGridCardProps {
  disabled: boolean;
  mode?: StandardPlanGridCardMode;
  isCurrentPlan?: boolean;
  cancellationDate?: string;
}

export const StandardPlanGridCard: React.FC<StandardPlanGridCardProps> = ({
  disabled,
  mode = "subscribe",
  isCurrentPlan = false,
  cancellationDate,
}) => {
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
      text: <StandardDowngradeConsequences />,
      submitButtonText: "plans.standard.downgrade.confirmSubmit",
      cancelButtonText: "plans.standard.downgrade.confirmCancel",
      onSubmit: () => {
        closeConfirmationModal();
        goToCustomerPortal();
      },
    });
  };

  return (
    <PlanGridCard
      data-testid="standard-plan-card"
      banner={
        <Message
          type="info"
          data-testid="pricing-changes-banner"
          text={
            <>
              <FormattedMessage id="planGrid.standard.pricingChanges.banner" />
              <ul className={styles.pricingChangesList}>
                <li>
                  <FormattedMessage id="planGrid.standard.pricingChanges.minimum" />
                </li>
                <li>
                  <FormattedMessage id="planGrid.standard.pricingChanges.beyond" />
                </li>
              </ul>
            </>
          }
        />
      }
      title={<FormattedMessage id="plans.standard.title" />}
      price={<FormattedMessage id="planGrid.standard.price" />}
      pricePeriod={<FormattedMessage id="planGrid.perMonth" />}
      description={<FormattedMessage id="planGrid.standard.description" />}
      features={
        <PlanGridFeatureList
          messageId="planGrid.standard.features"
          values={{
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
          <Button
            full
            isLoading={redirecting}
            disabled={disabled}
            variant={isDowngrade ? "secondary" : "primary"}
            onClick={onClick}
          >
            <FormattedMessage id={isDowngrade ? "plans.standard.downgrade" : "plans.standard.subscribe"} />
          </Button>
        )
      }
    />
  );
};
