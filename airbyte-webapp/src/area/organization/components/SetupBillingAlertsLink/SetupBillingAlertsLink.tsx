import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useLinkToBillingPage } from "cloud/area/billing/utils/useLinkToBillingPage";
import { useShowsBillingAlertsSection } from "cloud/area/billing/utils/useShowsBillingAlertsSection";

export const SetupBillingAlertsLink: React.FC = () => {
  const showsBillingAlertsSection = useShowsBillingAlertsSection();
  const billingPagePath = useLinkToBillingPage();

  if (!showsBillingAlertsSection) {
    return null;
  }

  return (
    <Text size="sm">
      <Link to={billingPagePath}>
        <FlexContainer alignItems="center" gap="xs">
          <Icon type="bell" size="sm" />
          <FormattedMessage id="settings.organization.billing.setupNotifications" />
        </FlexContainer>
      </Link>
    </Text>
  );
};
