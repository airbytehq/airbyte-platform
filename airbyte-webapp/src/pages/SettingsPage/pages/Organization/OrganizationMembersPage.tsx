import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { OrganizationAccessManagementSection } from "../AccessManagementPage/OrganizationAccessManagementSection";

export const OrganizationMembersPage = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATIONMEMBERS);

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center" wrap="wrap">
        <FlexItem grow>
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.members.title" />
          </Heading>
        </FlexItem>
      </FlexContainer>
      <OrganizationAccessManagementSection />
    </FlexContainer>
  );
};
