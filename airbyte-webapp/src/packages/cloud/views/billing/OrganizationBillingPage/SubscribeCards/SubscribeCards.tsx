import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useOrgInfo } from "core/api";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
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
        <Heading as="h3" size="md">
          <FormattedMessage id="plans.cloud.title" />
        </Heading>
        <FlexContainer alignItems="center">
          <Text size="lg">
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
        <Heading as="h3" size="md">
          <FormattedMessage id="plans.teams.title" />
        </Heading>
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
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};
  return (
    <Box className={styles.subscribe} p="xl">
      <FlexContainer direction="column" gap="xl">
        <Heading as="h2" size="md">
          <FormattedMessage id="settings.organization.billing.subscribeTitle" />
        </Heading>
        <FlexContainer wrap="wrap" className={styles.subscribe__cards}>
          <CloudCard disabled={billing?.paymentStatus === "locked"} />
          <TeamsCard />
        </FlexContainer>
        <FlexItem>
          <ExternalLink href={links.pricingPage} opensInNewTab>
            <Button variant="clear" size="sm" icon="share" iconPosition="right" iconSize="sm">
              <FormattedMessage id="settings.organization.billing.pricingFeatureComparison" />
            </Button>
          </ExternalLink>
        </FlexItem>
      </FlexContainer>
    </Box>
  );
};
