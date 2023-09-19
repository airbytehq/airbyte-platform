import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React, { useEffect, useRef, useState } from "react";
import { FormattedMessage, FormattedNumber } from "react-intl";
import { useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useIsFCPEnabled, useStripeCheckout } from "core/api/cloud";
import { useGetCloudWorkspace, useInvalidateCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceRead } from "core/api/types/CloudApi";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import { EmailVerificationHint } from "./EmailVerificationHint";
import { LowCreditBalanceHint } from "./LowCreditBalanceHint";
import styles from "./RemainingCredits.module.scss";
import { useBillingPageBanners } from "../useBillingPageBanners";
import { useDeprecatedBillingPageBanners } from "../useDeprecatedBillingPageBanners";

const STRIPE_SUCCESS_QUERY = "stripeCheckoutSuccess";

/**
 * Checks whether the given cloud workspace had a recent increase in credits.
 */
function hasRecentCreditIncrease(cloudWorkspace: CloudWorkspaceRead): boolean {
  const lastIncrement = cloudWorkspace.lastCreditPurchaseIncrementTimestamp;
  return lastIncrement ? Date.now() - lastIncrement < 30000 : false;
}

export const RemainingCredits: React.FC = () => {
  const { sendEmailVerification } = useAuthService();
  const retryIntervalId = useRef<number>();
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);
  const [searchParams, setSearchParams] = useSearchParams();
  const invalidateCloudWorkspace = useInvalidateCloudWorkspace(currentWorkspace.workspaceId);
  const { isLoading, mutateAsync: createCheckout } = useStripeCheckout();
  const analytics = useAnalyticsService();
  const [isWaitingForCredits, setIsWaitingForCredits] = useState(false);
  const billingPageBannerHook = useBillingPageBanners;
  const deprecatedBillingPageBannerHook = useDeprecatedBillingPageBanners;

  const isFCPEnabled = useIsFCPEnabled();
  const { bannerVariant } = isFCPEnabled ? deprecatedBillingPageBannerHook() : billingPageBannerHook();

  const { emailVerified } = useAuthService();

  useEffectOnce(() => {
    // If we are coming back from a successful stripe checkout ...
    if (searchParams.has(STRIPE_SUCCESS_QUERY)) {
      // Remove the stripe parameter from the URL
      setSearchParams({}, { replace: true });
      // If the workspace doesn't have a recent increase in credits our server has not yet
      // received the Stripe callback or updated the workspace information. We're going to
      // switch into a loading mode and reload the workspace every 3s from now on until
      // the workspace has received the credit update (see useEffect below)
      if (!hasRecentCreditIncrease(cloudWorkspace)) {
        setIsWaitingForCredits(true);
        retryIntervalId.current = window.setInterval(() => {
          invalidateCloudWorkspace();
        }, 3000);
      }
    }

    return () => clearInterval(retryIntervalId.current);
  });

  useEffect(() => {
    // Whenever the `cloudWorkspace` changes and now has a recent credit increment, while we're still waiting
    // for new credits to come in (i.e. the retryIntervalId is still set), we know that we now
    // handled the actual credit purchase and can clean the interval and loading state.
    if (retryIntervalId.current && hasRecentCreditIncrease(cloudWorkspace)) {
      clearInterval(retryIntervalId.current);
      retryIntervalId.current = undefined;
      setIsWaitingForCredits(false);
    }
  }, [cloudWorkspace]);

  const startStripeCheckout = async () => {
    // Use the current URL as a success URL but attach the STRIPE_SUCCESS_QUERY to it
    const successUrl = new URL(window.location.href);
    successUrl.searchParams.set(STRIPE_SUCCESS_QUERY, "true");
    const { stripeUrl } = await createCheckout({
      workspaceId: currentWorkspace.workspaceId,
      successUrl: successUrl.href,
      cancelUrl: window.location.href,
      stripeMode: "payment",
    });
    analytics.track(Namespace.CREDITS, Action.CHECKOUT_START, {
      actionDescription: "Checkout Start",
    });
    // Forward to stripe as soon as we created a checkout session successfully
    window.location.assign(stripeUrl);
  };

  return (
    <Card
      className={classNames({
        [styles.error]: bannerVariant === "error",
        [styles.warning]: bannerVariant === "warning",
        [styles.info]: bannerVariant === "info",
      })}
    >
      <Box p="xl">
        <FlexContainer alignItems="center" justifyContent="space-between">
          <FlexItem>
            <FlexContainer alignItems="baseline">
              <Text size="xl" as="span">
                <FormattedMessage id="credits.remainingCredits" />
              </Text>
              <ExternalLink href={links.creditDescription} variant="primary">
                <Text size="sm" as="span">
                  <FormattedMessage id="credits.whatAre" />
                </Text>
              </ExternalLink>
            </FlexContainer>

            <Text size="xl" bold>
              <FormattedNumber
                value={cloudWorkspace.remainingCredits ?? 0}
                maximumFractionDigits={2}
                minimumFractionDigits={2}
              />
            </Text>
          </FlexItem>
          <FlexContainer>
            <Button
              variant="dark"
              disabled={!emailVerified}
              type="button"
              size="xs"
              onClick={startStripeCheckout}
              isLoading={isLoading || isWaitingForCredits}
              icon={<FontAwesomeIcon icon={faPlus} />}
            >
              <FormattedMessage id="credits.buyCredits" />
            </Button>
            <Button size="xs" onClick={() => window.open(links.contactSales, "_blank")} variant="dark">
              <FormattedMessage id="credits.talkToSales" />
            </Button>
          </FlexContainer>
        </FlexContainer>
        <FlexContainer direction="column">
          {!emailVerified && sendEmailVerification && (
            <EmailVerificationHint variant={bannerVariant} sendEmailVerification={sendEmailVerification} />
          )}
          <LowCreditBalanceHint variant={bannerVariant} />
        </FlexContainer>
      </Box>
    </Card>
  );
};
