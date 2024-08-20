import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

export const OrganizationBillingPage: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organization.billing.title" />
      </Heading>
    </FlexContainer>
  );
};
