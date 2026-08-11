import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

export const OrganizationGroupsPage = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_GROUPS);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organization.groups.title" />
      </Heading>
    </FlexContainer>
  );
};
