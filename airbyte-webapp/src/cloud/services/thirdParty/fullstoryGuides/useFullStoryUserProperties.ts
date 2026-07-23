import { useEffect } from "react";

import { useCurrentOrganizationInfo, useCurrentWorkspaceOrUndefined } from "core/api";
import { useAuthService } from "core/services/auth";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { fullStorySetIdentity, fullStorySetUserProperties } from "core/utils/fullstory";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { useFullStoryGuidesReady } from "./FullStoryGuidesProvider";

export const useFullStoryUserProperties = () => {
  const isReady = useFullStoryGuidesReady();
  const { user, provider } = useAuthService();
  const workspace = useCurrentWorkspaceOrUndefined();
  const organizationInfo = useCurrentOrganizationInfo();
  const { subscriptionStatus, accountType, isInTrial, trialStatus } = useOrganizationSubscriptionStatus();

  const userId = user?.userId;
  const email = user?.email;
  const name = user?.name;
  const workspaceId = workspace?.workspaceId;
  const organizationId = workspace?.organizationId;
  const customerId = workspace?.customerId;
  const organizationPlanId = organizationInfo?.organizationPlanId;

  useEffect(() => {
    if (!isReady || !userId) {
      return;
    }

    fullStorySetIdentity(userId);
  }, [isReady, userId]);

  useEffect(() => {
    if (!isReady || !workspaceId || !userId || !email) {
      return;
    }

    fullStorySetUserProperties({
      email,
      name,
      provider,
      isCorporate: isCorporateEmail(email),
      workspaceId,
      organizationId,
      customerId,
      organizationPlanId,
      subscriptionStatus,
      accountType,
      isInTrial,
      trialStatus,
    });
  }, [
    isReady,
    userId,
    email,
    name,
    provider,
    workspaceId,
    organizationId,
    customerId,
    organizationPlanId,
    subscriptionStatus,
    accountType,
    isInTrial,
    trialStatus,
  ]);
};
