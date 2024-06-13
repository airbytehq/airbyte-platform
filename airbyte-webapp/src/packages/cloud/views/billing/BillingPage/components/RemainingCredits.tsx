import React, { useEffect, useRef, useState } from "react";
import { FormattedMessage, FormattedNumber, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetCloudWorkspace, useInvalidateCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceRead } from "core/api/types/CloudApi";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import { BillingBanners } from "./BillingBanners";
import { CheckoutCreditsModal } from "./CheckoutCreditsModal";

export const STRIPE_SUCCESS_QUERY = "stripeCheckoutSuccess";

/**
 * Checks whether the given cloud workspace had a recent increase in credits.
 */
function hasRecentCreditIncrease(cloudWorkspace: CloudWorkspaceRead): boolean {
  const lastIncrement = cloudWorkspace.lastCreditPurchaseIncrementTimestamp;
  return lastIncrement ? Date.now() - lastIncrement < 30000 : false;
}

export const RemainingCredits: React.FC = () => {
  const { formatMessage } = useIntl();
  const retryIntervalId = useRef<number>();
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);
  const [searchParams, setSearchParams] = useSearchParams();
  const invalidateCloudWorkspace = useInvalidateCloudWorkspace(currentWorkspace.workspaceId);
  const [isWaitingForCredits, setIsWaitingForCredits] = useState(false);
  const { openModal } = useModalService();
  const canBuyCredits = useIntent("BuyCredits", { workspaceId: currentWorkspace.workspaceId });

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

  const showCreditsModal = async () => {
    await openModal({
      title: formatMessage({ id: "credits.checkoutModalTitle" }),
      size: "md",
      content: CheckoutCreditsModal,
    });
  };

  return (
    <Card>
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
          <Button variant="secondaryDark" size="xs" onClick={() => window.open(links.contactSales, "_blank")}>
            <FormattedMessage id="credits.talkToSales" />
          </Button>
          <Button
            variant="primaryDark"
            disabled={!emailVerified || !canBuyCredits}
            type="button"
            size="xs"
            onClick={showCreditsModal}
            isLoading={isWaitingForCredits}
            icon="plus"
          >
            <FormattedMessage id="credits.buyCredits" />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <BillingBanners />
    </Card>
  );
};
