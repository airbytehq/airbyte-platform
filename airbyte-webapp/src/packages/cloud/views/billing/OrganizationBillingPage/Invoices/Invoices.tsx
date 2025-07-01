import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetInvoices } from "core/api";

import { InvoiceGrid, InvoiceGridLoadingSkeleton } from "./InvoiceGrid";
import { useRedirectToCustomerPortal } from "../../../../area/billing/utils/useRedirectToCustomerPortal";
import { UpdateButton } from "../UpdateButton";

export const Invoices = () => {
  const { redirecting, goToCustomerPortal } = useRedirectToCustomerPortal("portal");
  const organizationId = useCurrentOrganizationId();
  const { data, isLoading, isError } = useGetInvoices(organizationId);
  const invoices = data?.invoices;

  return (
    <>
      <FlexContainer alignItems="baseline">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.invoices" />
        </Heading>
        {invoices?.length && invoices.length > 0 ? (
          <UpdateButton onClick={goToCustomerPortal} isLoading={redirecting}>
            <FormattedMessage id="settings.organization.billing.invoices.viewAll" />
          </UpdateButton>
        ) : null}
      </FlexContainer>
      <Box pt="xl">
        {invoices ? (
          invoices.length > 0 ? (
            <InvoiceGrid invoices={invoices} hasMore={data.hasMore} />
          ) : (
            <Box py="2xl">
              <EmptyState text={<FormattedMessage id="settings.organization.billing.invoices.noInvoicesYet" />} />
            </Box>
          )
        ) : (
          <>
            {isLoading && <InvoiceGridLoadingSkeleton />}
            {isError && (
              <DataLoadingError>
                <FormattedMessage id="settings.organization.billing.invoicesError" />
              </DataLoadingError>
            )}
          </>
        )}
      </Box>
    </>
  );
};
