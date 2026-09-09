import React from "react";
import { FormattedDate, FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Separator } from "components/ui/Separator";
import { Text } from "components/ui/Text";

import styles from "./PlanGridCard.module.scss";

interface PlanGridCardProps {
  title: React.ReactNode;
  price: React.ReactNode;
  pricePeriod?: React.ReactNode;
  description: React.ReactNode;
  features: React.ReactNode;
  /** Bottom-pinned call to action. */
  cta: React.ReactNode;
  isCurrentPlan?: boolean;
  cancellationDate?: string;
  "data-testid"?: string;
}

export const PlanGridCard: React.FC<PlanGridCardProps> = ({
  title,
  price,
  pricePeriod,
  description,
  features,
  cta,
  isCurrentPlan = false,
  cancellationDate,
  "data-testid": testId,
}) => {
  return (
    <div className={styles.planGridCard} data-testid={testId}>
      <FlexContainer justifyContent="space-between" alignItems="center" gap="sm">
        <Heading as="h3" size="md">
          {title}
        </Heading>
        {isCurrentPlan && (
          <FlexContainer alignItems="center" gap="xs">
            <Badge variant="blue" uppercase={false} data-testid="current-plan-badge">
              <FormattedMessage id="planGrid.currentPlan" />
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
        )}
      </FlexContainer>
      <FlexContainer alignItems="baseline" gap="sm">
        <span className={styles.planGridCard__price}>{price}</span>
        {pricePeriod && (
          <Text as="span" size="lg" color="grey400">
            {pricePeriod}
          </Text>
        )}
      </FlexContainer>
      <Text size="lg" color="grey400">
        {description}
      </Text>
      <Separator />
      {features}
      <div className={styles.planGridCard__cta}>{cta}</div>
    </div>
  );
};

interface PlanGridFeatureListProps {
  messageId: string;
  values?: React.ComponentProps<typeof FormattedMessage>["values"];
}

export const PlanGridFeatureList: React.FC<PlanGridFeatureListProps> = ({ messageId, values }) => {
  return (
    <ul className={styles.features}>
      <FormattedMessage
        id={messageId}
        values={{
          li: (node: React.ReactNode) => (
            <li className={styles.feature}>
              <Icon type="check" size="sm" color="primary" />
              <Text as="span" size="lg">
                {node}
              </Text>
            </li>
          ),
          ...values,
        }}
      />
    </ul>
  );
};
