import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useUpdateWorkspace } from "core/api";
import { Geography } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useNotificationService } from "hooks/services/Notification";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

const schema = yup.object().shape({
  defaultGeography: yup.mixed<Geography>().optional(),
});

interface DefaultDataResidencyFormValues {
  defaultGeography?: Geography;
}

const fieldDescription = (
  <FormattedMessage
    id="settings.geographyDescription"
    values={{
      lnk: (node: React.ReactNode) => <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>,
      request: (node: React.ReactNode) => <ExternalLink href={links.dataResidencySurvey}>{node}</ExternalLink>,
    }}
  />
);

export const DataResidencyView: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  useTrackPage(PageTrackingCodes.SETTINGS_DATA_RESIDENCY);

  const handleSubmit = async (values: DefaultDataResidencyFormValues) => {
    await updateWorkspace({
      workspaceId: workspace.workspaceId,
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
    <Card title={<FormattedMessage id="settings.defaultDataResidency" />}>
      <Card withPadding>
        <FlexContainer direction="column">
          <Text color="grey300" size="sm">
            <FormattedMessage
              id="settings.defaultDataResidencyDescription"
              values={{
                lnk: (node: React.ReactNode) => <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>,
              }}
            />
          </Text>
          <Form<DefaultDataResidencyFormValues>
            defaultValues={{
              defaultGeography: workspace.defaultGeography,
            }}
            schema={schema}
            onSubmit={handleSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <DataResidencyDropdown<DefaultDataResidencyFormValues>
              labelId="settings.defaultGeography"
              description={fieldDescription}
              name="defaultGeography"
              inline
            />
            <FormSubmissionButtons submitKey="form.saveChanges" />
          </Form>
        </FlexContainer>
      </Card>
    </Card>
  );
};
