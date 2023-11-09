import { FormattedMessage } from "react-intl";

import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";

interface PbaBillingBannerProps {
  organizationName: string;
}

export const PbaBillingBanner: React.FC<PbaBillingBannerProps> = ({ organizationName }) => {
  return (
    <Message
      text={
        <FormattedMessage
          id="billing.pbaBillingActive"
          values={{
            organizationName,
            lnk: (node: React.ReactNode) => (
              <ExternalLink href="mailto:billing@airbyte.io" variant="primary">
                {node}
              </ExternalLink>
            ),
          }}
        />
      }
    />
  );
};
