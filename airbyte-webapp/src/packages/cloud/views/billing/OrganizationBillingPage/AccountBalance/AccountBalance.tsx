import dayjs from "dayjs";
import React, { useMemo } from "react";
import { FormattedDate, FormattedMessage, FormattedNumber, useIntl } from "react-intl";

import { BorderedTile } from "components/ui/BorderedTiles";
import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetOrganizationSubscriptionInfo } from "core/api";
import { CreditBlockRead } from "core/api/types/AirbyteClient";
import { useFormatCredits } from "core/utils/numberHelper";

export const AccountBalance = () => {
  const organizationId = useCurrentOrganizationId();
  const {
    data: subscriptionInfo,
    isLoading: balanceIsLoading,
    isError: balanceError,
  } = useGetOrganizationSubscriptionInfo(organizationId);
  const { formatCredits } = useFormatCredits();

  const hasPositiveCreditBalance = !!subscriptionInfo?.credits?.balance && subscriptionInfo.credits.balance > 0;

  if (subscriptionInfo?.balanceHidden) {
    return null;
  }

  return (
    <BorderedTile>
      <Heading as="h2" size="sm">
        <FormattedMessage id="settings.organization.billing.accountBalance" />
      </Heading>
      <Box pt="xl">
        {balanceIsLoading && (
          <FlexContainer direction="column" gap="sm">
            <LoadingSkeleton />
            <LoadingSkeleton />
          </FlexContainer>
        )}
        {subscriptionInfo && (
          <FlexContainer justifyContent="space-between" gap="lg" direction="column">
            {hasPositiveCreditBalance && (
              <FlexItem>
                <FlexContainer alignItems="center" gap="xs">
                  <Text>
                    <FormattedMessage id="settings.organization.billing.remainingCredits" />
                  </Text>
                  {!!subscriptionInfo?.credits?.blocks?.length && (
                    <CreditExpiryTooltip creditBlocks={subscriptionInfo.credits.blocks} />
                  )}
                </FlexContainer>

                <Text size="lg">
                  <FormattedMessage
                    id="settings.organization.billing.remainingCreditsAmount"
                    values={{ amount: formatCredits(subscriptionInfo.credits?.balance ?? 0) }}
                  />
                </Text>
              </FlexItem>
            )}
            {subscriptionInfo.upcomingInvoice && (
              <>
                <FlexItem>
                  <Text size="sm">
                    <FormattedMessage id="settings.organization.billing.upcomingInvoiceAmount" />
                  </Text>
                  <Text size="lg">
                    <FormattedNumber
                      value={
                        isNaN(parseFloat(subscriptionInfo.upcomingInvoice.amount))
                          ? 0
                          : parseFloat(subscriptionInfo.upcomingInvoice.amount)
                      }
                      currency={subscriptionInfo.upcomingInvoice.currency}
                      style="currency"
                      minimumFractionDigits={2}
                      maximumFractionDigits={2}
                    />
                  </Text>
                </FlexItem>
                <FlexItem>
                  <Text size="sm">
                    <FormattedMessage id="settings.organization.billing.invoiceDate" />
                  </Text>
                  <Text size="lg">
                    <FormattedDate
                      value={dayjs(subscriptionInfo.upcomingInvoice.dueDate).toDate()}
                      dateStyle="medium"
                    />
                  </Text>
                </FlexItem>
              </>
            )}
          </FlexContainer>
        )}
        {balanceError && (
          <DataLoadingError>
            <FormattedMessage id="settings.organization.billing.accountBalanceError" />
          </DataLoadingError>
        )}
      </Box>
    </BorderedTile>
  );
};

const CreditExpiryTooltip = ({ creditBlocks }: { creditBlocks: CreditBlockRead[] }) => {
  const { formatDate } = useIntl();
  const { formatCredits } = useFormatCredits();

  const sortedCreditBlocks = useMemo(
    () =>
      creditBlocks
        .filter((block) => block.amount && block.expiryDate)
        .sort((a, b) => (dayjs(a.expiryDate).isBefore(dayjs(b.expiryDate)) ? -1 : 1)),
    [creditBlocks]
  );

  if (!sortedCreditBlocks.length) {
    return null;
  }

  return (
    <Tooltip control={<Icon type="calendar" size="xs" />}>
      {sortedCreditBlocks.map((creditBlock, index) => (
        <React.Fragment key={index}>
          <FormattedMessage
            id="settings.organization.billing.creditBlockExpiry"
            values={{
              amount: formatCredits(creditBlock.amount),
              expiryDate: formatDate(creditBlock.expiryDate, { dateStyle: "medium" }),
            }}
          />
          <br />
        </React.Fragment>
      ))}
    </Tooltip>
  );
};
