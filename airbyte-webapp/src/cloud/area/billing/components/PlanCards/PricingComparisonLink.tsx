import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexItem } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

export const PricingComparisonLink: React.FC = () => {
  return (
    <FlexItem>
      <ExternalLink href={links.pricingPage} opensInNewTab>
        <Button variant="clear" size="sm" icon="share" iconPosition="right" iconSize="sm">
          <FormattedMessage id="settings.organization.billing.pricingFeatureComparison" />
        </Button>
      </ExternalLink>
    </FlexItem>
  );
};
