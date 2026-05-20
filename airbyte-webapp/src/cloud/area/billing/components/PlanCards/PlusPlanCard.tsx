import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { links } from "core/utils/links";

import { PlanCard } from "./PlanCard";
import styles from "./PlanCard.module.scss";

interface PlusPlanCardProps {
  disabled?: boolean;
  isPaidPlan?: boolean;
}

export const PlusPlanCard: React.FC<PlusPlanCardProps> = ({ disabled = false, isPaidPlan = false }) => {
  const { goToCustomerPortal } = useRedirectToCustomerPortal("setup", "plus");
  const [isLoading, setIsLoading] = useState(false);

  const ctaMessageId = isPaidPlan ? "plans.plus.upgrade" : "plans.plus.get";

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
          <Button
            isLoading={isLoading}
            disabled={disabled}
            variant="primary"
            onClick={async () => {
              setIsLoading(true);
              await goToCustomerPortal();
              setIsLoading(false);
            }}
          >
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
