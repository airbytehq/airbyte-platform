import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

import { PlanGridCard, PlanGridFeatureList } from "./PlanGridCard";
import styles from "./PlanGridCard.module.scss";

interface FlexPlanGridCardProps {
  disabled?: boolean;
  isCurrentPlan?: boolean;
  cancellationDate?: string;
}

export const FlexPlanGridCard: React.FC<FlexPlanGridCardProps> = ({
  disabled = false,
  isCurrentPlan = false,
  cancellationDate,
}) => {
  return (
    <PlanGridCard
      data-testid="flex-plan-card"
      title={<FormattedMessage id="plans.flex.title" />}
      price={<FormattedMessage id="planGrid.customPrice" />}
      description={<FormattedMessage id="plans.flex.description" />}
      features={<PlanGridFeatureList messageId="plans.flex.features" />}
      isCurrentPlan={isCurrentPlan}
      cancellationDate={cancellationDate}
      cta={
        isCurrentPlan || disabled ? (
          <Button full variant="primary" disabled>
            <FormattedMessage id="planGrid.talkToSales" />
          </Button>
        ) : (
          <ExternalLink href={links.contactSales} className={styles.planGridCard__ctaLink}>
            <Button full variant="primary">
              <FormattedMessage id="planGrid.talkToSales" />
            </Button>
          </ExternalLink>
        )
      }
    />
  );
};
