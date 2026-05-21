import { FormattedDate, FormattedMessage } from "react-intl";

import { BorderedTile } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink, Link } from "components/ui/Link";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useLinkToPlanPage } from "cloud/area/billing/utils/useLinkToPlanPage";
import { useGetOrganizationSubscriptionInfo } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { links } from "core/utils/links";

import { CancelSubscription } from "./CancelSubscription";
import styles from "./Subscription.module.scss";

export const Subscription: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const { data: subscription, isLoading, isError } = useGetOrganizationSubscriptionInfo(organizationId);
  const planPagePath = useLinkToPlanPage();
  const isSelfServePlusPlanEnabled = useExperiment("billing.selfServePlusPlan");

  return (
    <BorderedTile>
      <FlexContainer
        justifyContent="space-between"
        alignItems="center"
        wrap="wrap"
        className={styles.subscription__header}
      >
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.subscription" />
        </Heading>
        <div className={styles.subscription__changePlan}>
          <FlexContainer gap="sm" alignItems="center">
            {isSelfServePlusPlanEnabled && (
              <Link to={planPagePath} variant="button">
                <FormattedMessage id="settings.organization.billing.subscription.changePlan" />
              </Link>
            )}
            {subscription &&
              (subscription.selfServeSubscription ? (
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
              ))}
          </FlexContainer>
        </div>
      </FlexContainer>
      <Box pt="xl">
        {isLoading && <LoadingSkeleton />}
        {!isLoading && !isError && !subscription && (
          <Text size="lg" color="grey400">
            <FormattedMessage id="settings.organization.billing.subscription.noActivePlan" />
          </Text>
        )}
        {subscription && (
          <FlexContainer justifyContent="space-between" gap="lg" direction="column">
            {/* If the Stigg entitlement plan is set, use it instead of the Orb plan name */}
            <Text size="lg">{subscription?.entitlementPlan?.planName ?? subscription.name}</Text>
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
