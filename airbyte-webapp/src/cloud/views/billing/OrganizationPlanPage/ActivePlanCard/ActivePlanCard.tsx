import classNames from "classnames";
import React from "react";
import { FormattedDate, FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import styles from "./ActivePlanCard.module.scss";

export type ActivePlanTier = "standard" | "plus" | "pro";

interface ActivePlanCardProps {
  tier: ActivePlanTier;
  planName?: string;
  isLoading?: boolean;
  isError?: boolean;
  cancellationDate?: string;
}

const tierClassName: Record<ActivePlanTier, string> = {
  standard: styles["activePlanCard--standard"],
  plus: styles["activePlanCard--plus"],
  pro: styles["activePlanCard--pro"],
};

const tierDescriptorMessageId: Record<ActivePlanTier, string> = {
  standard: "settings.organization.billing.plan.activePlan.descriptor.standard",
  plus: "settings.organization.billing.plan.activePlan.descriptor.plus",
  pro: "settings.organization.billing.plan.activePlan.descriptor.pro",
};

const PlanNameDisplay: React.FC<Pick<ActivePlanCardProps, "planName" | "isLoading" | "isError">> = ({
  planName,
  isLoading,
  isError,
}) => {
  if (isError) {
    return (
      <DataLoadingError>
        <FormattedMessage id="settings.organization.billing.subscriptionError" />
      </DataLoadingError>
    );
  }
  if (isLoading || !planName) {
    return <LoadingSkeleton />;
  }
  return (
    <Heading as="h2" size="lg">
      {planName}
    </Heading>
  );
};

export const ActivePlanCard: React.FC<ActivePlanCardProps> = ({
  tier,
  planName,
  isLoading,
  isError,
  cancellationDate,
}) => {
  return (
    <div
      className={classNames(styles.activePlanCard, tierClassName[tier])}
      data-testid="active-plan-card"
      data-tier={tier}
    >
      <FlexContainer direction="column" gap="md">
        <FlexContainer alignItems="center" gap="sm">
          <Badge variant="green">
            <FormattedMessage id="settings.organization.billing.plan.activePlan.status.active" />
          </Badge>
          {cancellationDate && (
            <Badge variant="yellow">
              <FormattedMessage
                id="settings.organization.billing.plan.activePlan.status.cancelling"
                values={{ date: <FormattedDate value={cancellationDate} dateStyle="medium" /> }}
              />
            </Badge>
          )}
        </FlexContainer>
        <PlanNameDisplay planName={planName} isLoading={isLoading} isError={isError} />
        <Text color="grey400">
          <FormattedMessage id={tierDescriptorMessageId[tier]} />
        </Text>
      </FlexContainer>
    </div>
  );
};
