import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useUpdateWorkspace } from "core/api";
import { Geography } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

const schema = yup.object().shape({
  defaultGeography: yup.mixed<Geography>().optional(),
});

interface DefaultDataResidencyFormValues {
  defaultGeography?: Geography;
}

export const DataResidencyView: React.FC = () => {
  const { workspaceId, organizationId, defaultGeography } = useCurrentWorkspace();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId, organizationId });

  useTrackPage(PageTrackingCodes.SETTINGS_DATA_RESIDENCY);

  const handleSubmit = async (values: DefaultDataResidencyFormValues) => {
    await updateWorkspace({
      workspaceId,
      defaultGeography: values.defaultGeography,
    });
  };

  const onSuccess = () =>
    registerNotification({
      id: "workspaceSettings.defaultGeographySuccess",
      text: formatMessage({ id: "settings.defaultDataResidencyUpdateSuccess" }),
      type: "success",
    });

  const onError = (e: Error, { defaultGeography }: DefaultDataResidencyFormValues) => {
    trackError(e, { defaultGeography });
    registerNotification({
      id: "workspaceSettings.defaultGeographyError",
      text: formatMessage({ id: "settings.defaultDataResidencyUpdateError" }),
      type: "error",
    });
  };

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.defaultDataResidency" })}</Heading>
      <Text size="sm">
        <FormattedMessage
          id="settings.defaultDataResidencyDescription"
          values={{
            lnk: (node: React.ReactNode) => <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>,
            request: (node: React.ReactNode) => <ExternalLink href={links.dataResidencySurvey}>{node}</ExternalLink>,
          }}
        />
      </Text>
      <Form<DefaultDataResidencyFormValues>
        defaultValues={{
          defaultGeography,
        }}
        schema={schema}
        onSubmit={handleSubmit}
        onSuccess={onSuccess}
        onError={onError}
        disabled={!canUpdateWorkspace}
      >
        <DataResidencyDropdown<DefaultDataResidencyFormValues>
          labelId="settings.defaultGeography"
          name="defaultGeography"
        />
        <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />
      </Form>
    </FlexContainer>
  );
};
