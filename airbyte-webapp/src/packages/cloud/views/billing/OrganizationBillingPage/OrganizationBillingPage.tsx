import { FormattedMessage, useIntl } from "react-intl";

import { EmptyState } from "components/common/EmptyState";
import { PageContainer } from "components/PageContainer";
import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useCurrentOrganizationInfo } from "core/api";

import { BillingInformation } from "./BillingInformation";
import { Invoices } from "./Invoices";
import { PaymentMethod } from "./PaymentMethod";
import { useRedirectToCustomerPortal } from "../useRedirectToCustomerPortal";

export const OrganizationBillingPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { billing } = useCurrentOrganizationInfo();
  const { goToCustomerPortal, redirecting } = useRedirectToCustomerPortal("portal");

  return (
    <PageContainer>
      {billing && billing.paymentStatus !== "uninitialized" ? (
        <FlexContainer direction="column" gap="xl">
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.billing.title" />
          </Heading>

          <BorderedTiles>
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
      )}
    </PageContainer>
  );
};
