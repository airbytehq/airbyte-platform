import { FormattedDate, FormattedMessage } from "react-intl";

import { BorderedTile } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspace, useGetOrganizationSubscriptionInfo } from "core/api";
import { links } from "core/utils/links";

import { CancelSubscription } from "./CancelSubscription";
import styles from "./Subscription.module.scss";

export const Subscription: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();
  const { data: subscription, isLoading, isError } = useGetOrganizationSubscriptionInfo(organizationId);

  return (
    <BorderedTile>
      <FlexContainer justifyContent="space-between" className={styles.subscription__header}>
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.subscription" />
        </Heading>
        {subscription && (
          <div className={styles.subscription__changePlan}>
            {subscription.selfServeSubscription ? (
              <CancelSubscription subscription={subscription} disabled={false} />
            ) : (
              <Tooltip control={<CancelSubscription subscription={subscription} disabled />}>
                <FormattedMessage
                  id="settings.organization.billing.noSelfServePlan"
                  values={{
                    lnk: (node: React.ReactNode) => (
                      <ExternalLink opensInNewTab href={links.contactSales} variant="primary">
                        {node}
                      </ExternalLink>
                    ),
                  }}
                />
              </Tooltip>
            )}
          </div>
        )}
      </FlexContainer>
      <Box pt="xl">
        {isLoading && <LoadingSkeleton />}
        {subscription && (
          <FlexContainer justifyContent="space-between" gap="lg" direction="column">
            <Text size="lg">{subscription.name}</Text>
            {subscription.cancellationDate && (
              <FlexItem>
                <FlexContainer alignItems="center" gap="xs">
                  <Text>
                    <FormattedMessage id="settings.organization.billing.subscriptionCancelDate" />
                  </Text>
                </FlexContainer>

                <Text size="lg">
                  <FormattedDate value={subscription.cancellationDate} dateStyle="medium" />
                </Text>
              </FlexItem>
            )}
          </FlexContainer>
        )}
        {isError && (
          <DataLoadingError>
            <FormattedMessage id="settings.organization.billing.subscriptionError" />
          </DataLoadingError>
        )}
      </Box>
    </BorderedTile>
  );
};
