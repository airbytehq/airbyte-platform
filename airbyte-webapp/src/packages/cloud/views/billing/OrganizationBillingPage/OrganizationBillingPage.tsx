import { FormattedMessage, useIntl } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { PageContainer } from "components/PageContainer";
import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useGetOrganizationBillingBalance } from "core/api";
import { useFormatCredits } from "core/utils/numberHelper";

import { AccountBalance } from "./AccountBalance";
import { BillingBanners } from "./BillingBanners";
import { BillingInformation } from "./BillingInformation";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { useRedirectToCustomerPortal } from "../useRedirectToCustomerPortal";

export const OrganizationBillingPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { organizationId } = useCurrentWorkspace();
  const { billing } = useCurrentOrganizationInfo();
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("portal");
  const { formatCredits } = useFormatCredits();

  const { data: balance } = useGetOrganizationBillingBalance(organizationId);

  return (
    <PageContainer>
      {billing && billing.paymentStatus !== "uninitialized" ? (
        <FlexContainer direction="column" gap="xl">
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.billing.title" />
          </Heading>

          <BillingBanners />

          <BorderedTiles>
            <BorderedTile>
              <AccountBalance />
            </BorderedTile>

            <BorderedTile>
              <BillingInformation />
            </BorderedTile>

            <BorderedTile>
              <PaymentMethod />
            </BorderedTile>
          </BorderedTiles>

          <Box py="lg">
            <Invoices />
          </Box>
        </FlexContainer>
      ) : (
        <FlexContainer gap="2xl" direction="column">
          {!!balance?.credits?.balance && balance?.credits?.balance > 0 && (
            <Message
              text={
                <FormattedMessage
                  id="settings.organization.billing.remainingCreditsBanner"
                  values={{
                    amount: formatCredits(balance.credits.balance),
                  }}
                />
              }
            />
          )}
          <Box py="2xl">
            <EmptyState
              text={formatMessage({ id: "settings.organization.billing.notSetUp" })}
              button={
                <Button variant="primary" onClick={goToCustomerPortal} isLoading={redirecting}>
                  <FormattedMessage id="settings.organization.billing.paymentMethod.add" />
                </Button>
              }
            />
          </Box>
        </FlexContainer>
      )}
    </PageContainer>
  );
};
