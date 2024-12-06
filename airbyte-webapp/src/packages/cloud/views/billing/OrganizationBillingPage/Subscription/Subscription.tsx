import { FormattedDate, FormattedMessage } from "react-intl";

import { BorderedTile } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useGetOrganizationSubscriptionInfo } from "core/api";

export const Subscription: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();
  const { data: subscription, isLoading, isError } = useGetOrganizationSubscriptionInfo(organizationId);
  return (
    <BorderedTile>
      <Heading as="h2" size="sm">
        <FormattedMessage id="settings.organization.billing.plan" />
      </Heading>
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
                  <FormattedDate value={subscription.cancellationDate} />
                </Text>
              </FlexItem>
            )}
          </FlexContainer>
        )}
        {isError && (
          <DataLoadingError>
            <FormattedMessage id="settings.organization.billing.planError" />
          </DataLoadingError>
        )}
      </Box>
    </BorderedTile>
  );
};
