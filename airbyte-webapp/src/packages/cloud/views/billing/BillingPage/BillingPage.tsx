import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { SortOrderEnum } from "components/EntityTable/types";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { links } from "core/utils/links";

import styles from "./BillingPage.module.scss";
import { CreditsUsage } from "./components/CreditsUsage";
import { CreditsUsageContextProvider } from "./components/CreditsUsageContext";
import { RemainingCredits } from "./components/RemainingCredits";
import { ReactComponent as FilesIcon } from "./filesIcon.svg";

export interface BillingPageQueryParams {
  sortBy?: string;
  order?: SortOrderEnum;
}

const StripePortalLink: React.FC = () => {
  return (
    <a
      href={links.stripeCustomerPortal}
      rel="noopener noreferrer"
      target="_blank"
      className={classnames(styles.stripePortalLink, styles.button, styles.typeSecondary, styles.sizeS)}
    >
      <FlexContainer alignItems="center">
        <FilesIcon className={styles.filesIcon} />
        <FormattedMessage id="credits.stripePortalLink" />
      </FlexContainer>
    </a>
  );
};
export const BillingPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CREDITS);

  return (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "credits.billing" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="credits.billing" />
            </Heading>
          }
          endComponent={<StripePortalLink />}
        />
      }
    >
      <FlexContainer direction="column" className={styles.content}>
        <RemainingCredits />
        <React.Suspense
          fallback={
            <div className={styles.creditUsageLoading}>
              <Spinner size="sm" />
              <Text>
                <FormattedMessage id="credits.loadingCreditsUsage" />
              </Text>
            </div>
          }
        >
          <CreditsUsageContextProvider>
            <CreditsUsage />
          </CreditsUsageContextProvider>
        </React.Suspense>
      </FlexContainer>
    </MainPageWithScroll>
  );
};
