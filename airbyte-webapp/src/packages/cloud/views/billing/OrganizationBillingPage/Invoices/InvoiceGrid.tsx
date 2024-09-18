import React from "react";
import { FormattedDate, FormattedMessage, FormattedNumber } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { InvoiceRead, InvoiceReadStatus } from "core/api/types/AirbyteClient";

import styles from "./InvoiceGrid.module.scss";

interface InvoiceGridProps {
  invoices: InvoiceRead[];
  hasMore: boolean;
}

const invoiceStatusToBadgeVariant = (status: InvoiceReadStatus) => {
  switch (status) {
    case "paid":
      return "blue";
    case "open":
      return "green";
    case "draft":
    case "void":
    case "uncollectible":
    default:
      return "grey";
  }
};

/**
 * Convert the invoice amount to a decimal number. Some currencies are not returned as 1/100th
 * of the whole unit, but directly as the unit. Those won't need to be devided by 100.
 * See: https://docs.stripe.com/currencies#zero-decimal
 */
const toDecimal = (value: number, currency: string) => {
  if (
    [
      "bif",
      "clp",
      "djf",
      "gnf",
      "jpy",
      "kmf",
      "krw",
      "mga",
      "pyg",
      "rwf",
      "ugx",
      "vnd",
      "vuv",
      "xaf",
      "xof",
      "xpf",
    ].includes(currency.toLowerCase())
  ) {
    return value;
  }
  return value / 100;
};

export const InvoiceGrid: React.FC<InvoiceGridProps> = ({ invoices, hasMore }) => {
  return (
    <>
      <div className={styles.invoiceGrid}>
        {invoices?.map((invoice, index) => (
          <React.Fragment key={invoice.id}>
            <Text>
              <FormattedDate value={invoice.invoiceDate * 1000} dateStyle="medium" />
            </Text>

            <Text bold>{invoice.number}</Text>

            <Text>
              <FormattedNumber
                value={toDecimal(invoice.total, invoice.currency)}
                style="currency"
                currency={invoice.currency}
              />
            </Text>

            <Badge variant={invoiceStatusToBadgeVariant(invoice.status)}>{invoice.status}</Badge>

            <div className={styles.invoiceGrid__open}>
              <Text>
                <ExternalLink href={invoice.invoiceUrl} opensInNewTab className={styles.invoiceGrid__openLink}>
                  <FlexContainer alignItems="center" gap="sm">
                    <FormattedMessage id="settings.organization.billing.invoices.viewInvoice" />
                  </FlexContainer>
                </ExternalLink>
              </Text>
            </div>

            {index !== invoices.length - 1 && <div className={styles.invoiceGrid__separator} />}
          </React.Fragment>
        ))}
      </div>
      {hasMore && (
        <Box mt="xl">
          <Text color="grey" size="sm">
            <FormattedMessage
              id="settings.organization.billing.invoices.moreInvoices"
              values={{
                viewAll: (
                  <strong>
                    <FormattedMessage id="settings.organization.billing.invoices.viewAll" />
                  </strong>
                ),
              }}
            />
          </Text>
        </Box>
      )}
    </>
  );
};

export const InvoiceGridLoadingSkeleton = () => {
  return (
    <div className={styles.invoiceGrid}>
      <LoadingSkeleton className={styles.invoiceGrid__loading} />
      <div className={styles.invoiceGrid__separator} />
      <LoadingSkeleton className={styles.invoiceGrid__loading} />
      <div className={styles.invoiceGrid__separator} />
      <LoadingSkeleton className={styles.invoiceGrid__loading} />
    </div>
  );
};
