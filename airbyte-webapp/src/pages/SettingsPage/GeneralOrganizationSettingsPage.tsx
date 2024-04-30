import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";

import { useCurrentWorkspace } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

import { OrganizationAccessManagementSection } from "./pages/AccessManagementPage/OrganizationAccessManagementSection";
import { UpdateOrganizationSettingsForm } from "./UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION);
  const { formatMessage } = useIntl();
  const { organizationId } = useCurrentWorkspace();
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);

  return (
    <FlexContainer direction="column" gap="xl">
      <FlexContainer alignItems="center" wrap="wrap">
        <FlexItem grow>
          <Heading as="h1" size="md">
            <FormattedMessage id="settings.organization.general.title" />
          </Heading>
        </FlexItem>
        <CopyButton
          content={organizationId}
          variant="clear"
          iconPosition="right"
          title={formatMessage({ id: "settings.organizationSettings.copyOrgId" })}
        >
          <FormattedMessage id="settings.organizationSettings.orgId" values={{ id: organizationId }} />
        </CopyButton>
      </FlexContainer>
      <UpdateOrganizationSettingsForm />

      {isAccessManagementEnabled && displayOrganizationUsers && (
        <>
          <Separator />
          <OrganizationAccessManagementSection />
        </>
      )}
    </FlexContainer>
  );
};
