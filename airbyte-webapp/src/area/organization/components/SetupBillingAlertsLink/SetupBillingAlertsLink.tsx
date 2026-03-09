import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useOrganization, useOrgInfo } from "core/api";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export const SetupBillingAlertsLink: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const { email } = useOrganization(organizationId);
  const { billing } = useOrgInfo(organizationId, canManageOrganizationBilling) || {};

  const isSubscribedWithPayment =
    billing?.subscriptionStatus === "subscribed" && billing.paymentStatus !== "uninitialized";

  if (!isSubscribedWithPayment) {
    return null;
  }

  return (
    <Text size="sm">
      <ExternalLink
        href={links.billingNotificationsForm
          .replace("{organizationId}", organizationId)
          .replace("{email}", email ?? "")}
        opensInNewTab
      >
        <FlexContainer alignItems="center" gap="xs">
          <Icon type="bell" size="sm" />
          <FormattedMessage id="settings.organization.billing.setupNotifications" />
        </FlexContainer>
      </ExternalLink>
    </Text>
  );
};
