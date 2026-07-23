import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import { PlanCard } from "./PlanCard";
import styles from "./PlanCard.module.scss";

export const ProPlanCard: React.FC = () => {
  return (
    <PlanCard variant="clear">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Heading as="h3" size="md">
          <FormattedMessage id="plans.pro.title" />
        </Heading>
        <ExternalLink variant="button" href={links.contactSales}>
          <FormattedMessage id="plans.pro.contact" />
        </ExternalLink>
      </FlexContainer>
      <Text as="div" size="lg" className={styles.planCardText}>
        <p>
          <FormattedMessage id="plans.pro.description" />
        </p>
        <ul>
          <FormattedMessage
            id="plans.pro.features"
            values={{
              li: (node: React.ReactNode) => <li>{node}</li>,
            }}
          />
        </ul>
      </Text>
    </PlanCard>
  );
};
