import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { DiagnosticsButton } from "pages/SettingsPage/components/DiagnosticButton";
import { RegionsTable } from "pages/SettingsPage/components/RegionsTable";
import { UpdateSSOSettingsForm } from "pages/SettingsPage/UpdateSSOSettingsForm";

import { UpdateOrganizationSettingsForm } from "../../UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION);
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const isDownloadDiagnosticsFlagEnabled = useExperiment("settings.downloadDiagnostics");
  const isDownloadDiagnosticsFeatureEnabled = useFeature(FeatureItem.DiagnosticsExport);
  const supportsSSO = useFeature(FeatureItem.AllowUpdateSSOConfig);
  const supportsRegionsTable = useFeature(FeatureItem.AllowChangeDataplanes);

  // if EITHER flag OR feature is enabled, provide diagnostics
  // effectively: flag controls OSS+Cloud, feature controls SME
  const isDownloadDiagnosticsEnabled = isDownloadDiagnosticsFlagEnabled || isDownloadDiagnosticsFeatureEnabled;

  const canDownloadDiagnostics = useIntent("DownloadDiagnostics", { organizationId }) && isDownloadDiagnosticsEnabled;

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

      {supportsSSO && (
        <>
          <Separator />
          <UpdateSSOSettingsForm />
        </>
      )}

      {supportsRegionsTable && (
        <>
          <Separator />
          <RegionsTable />
        </>
      )}

      {canDownloadDiagnostics && (
        <>
          <Separator />
          <FlexContainer direction="column" gap="md">
            <Heading as="h2" size="sm">
              <FormattedMessage id="settings.organization.diagnostics.title" />
            </Heading>
            <Text color="grey" size="sm">
              <FormattedMessage id="settings.organization.diagnostics.description" />
            </Text>
            <FlexItem>
              <DiagnosticsButton />
            </FlexItem>
          </FlexContainer>
        </>
      )}
    </FlexContainer>
  );
};
