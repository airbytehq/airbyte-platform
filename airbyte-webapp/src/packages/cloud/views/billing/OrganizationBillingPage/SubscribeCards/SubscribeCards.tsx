import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo } from "core/api";
import { links } from "core/utils/links";
import { useRedirectToCustomerPortal } from "packages/cloud/area/billing/utils/useRedirectToCustomerPortal";

import styles from "./SubscribeCards.module.scss";

interface CardProps {
  variant: "primary" | "clear";
}

const Card: React.FC<React.PropsWithChildren<CardProps>> = ({ variant, children }) => {
  return (
    <Box
      px="xl"
      py="lg"
      className={classNames(styles.subscribe__card, {
        [styles["subscribe__card--primary"]]: variant === "primary",
      })}
    >
      {children}
    </Box>
  );
};

const CloudCard: React.FC<{ disabled: boolean }> = ({ disabled }) => {
  const { goToCustomerPortal } = useRedirectToCustomerPortal("setup");
  const [isLoading, setIsLoading] = useState(false);
  return (
    <Card variant="primary">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Text size="xl">
          <FormattedMessage id="plans.cloud.title" />
        </Text>
        <FlexContainer>
          <Text size="xl" className={styles.subscribe__price}>
            <FormattedMessage id="plans.cloud.price" />
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
            <FormattedMessage id="plans.cloud.subscribe" />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <Text size="lg" className={styles.subscribe__cardText}>
        <p>
          <FormattedMessage id="plans.cloud.description" />
        </p>
        <ul>
          <FormattedMessage
            id="plans.cloud.features"
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
    </Card>
  );
};

const TeamsCard: React.FC = () => {
  return (
    <Card variant="clear">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Text size="xl">
          <FormattedMessage id="plans.teams.title" />
        </Text>
        <ExternalLink variant="button" href={links.contactSales}>
          <FormattedMessage id="plans.teams.contact" />
        </ExternalLink>
      </FlexContainer>
      <Text size="lg" className={styles.subscribe__cardText}>
        <p>
          <FormattedMessage id="plans.teams.description" />
        </p>
        <ul>
          <FormattedMessage
            id="plans.teams.features"
            values={{
              li: (node: React.ReactNode) => <li>{node}</li>,
            }}
          />
        </ul>
      </Text>
    </Card>
  );
};

export const SubscribeCards: React.FC = () => {
  const { billing } = useCurrentOrganizationInfo();
  return (
    <Box className={styles.subscribe} p="xl">
      <FlexContainer direction="column" gap="xl">
        <Text size="xl" as="span" className={styles.subscribe__title}>
          <FormattedMessage id="settings.organization.billing.subscribeTitle" />
        </Text>
        <FlexContainer wrap="wrap" className={styles.subscribe__cards}>
          <CloudCard disabled={billing?.paymentStatus === "locked"} />
          <TeamsCard />
        </FlexContainer>
        <FlexItem>
          <Text size="lg">
            <ExternalLink href={links.pricingPage} opensInNewTab withIcon>
              <FormattedMessage id="settings.organization.billing.pricingFeatureComparison" />
            </ExternalLink>
          </Text>
        </FlexItem>
      </FlexContainer>
    </Box>
  );
};
