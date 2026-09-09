import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

import { PlanGridCard, PlanGridFeatureList } from "./PlanGridCard";
import styles from "./PlanGridCard.module.scss";

interface ProPlanGridCardProps {
  disabled?: boolean;
  isCurrentPlan?: boolean;
  cancellationDate?: string;
}

export const ProPlanGridCard: React.FC<ProPlanGridCardProps> = ({
  disabled = false,
  isCurrentPlan = false,
  cancellationDate,
}) => {
  return (
    <PlanGridCard
      data-testid="pro-plan-card"
      title={<FormattedMessage id="plans.pro.title" />}
      price={<FormattedMessage id="planGrid.customPrice" />}
      description={<FormattedMessage id="planGrid.pro.description" />}
      features={<PlanGridFeatureList messageId="planGrid.pro.features" />}
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
