import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useDbtCloudServiceToken } from "core/api/cloud";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useNotificationService } from "hooks/services/Notification";

import { useDbtTokenRemovalModal } from "./useDbtTokenRemovalModal";

const ServiceTokenFormSchema = yup.object().shape({
  serviceToken: yup.string().trim().required("form.empty.error"),
});

interface ServiceTokenFormValues {
  serviceToken: string;
}

export const cleanedErrorMessage = (e: Error): string => e.message.replace("Internal Server Error: ", "");
// a centrally-defined key for accessing the token value within formik objects

export const DbtCloudSettingsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const { hasExistingToken, saveToken } = useDbtCloudServiceToken();
  const { registerNotification } = useNotificationService();

  const onDeleteClick = useDbtTokenRemovalModal();

  const onSubmit = async ({ serviceToken }: ServiceTokenFormValues) => {
    await saveToken(serviceToken);
  };

  const onSuccess = () => {
    registerNotification({
      id: "dbtCloud/save-token-success",
      text: formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.actions.submit.success" }),
      type: "success",
    });
  };

  const onError = (e: Error) => {
    trackError(e);
    registerNotification({
      id: "dbtCloud/save-token-failure",
      text: cleanedErrorMessage(e),
      type: "error",
    });
  };

  return (
    <Card title={<FormattedMessage id="settings.integrationSettings.dbtCloudSettings" />}>
      <Card withPadding>
        <FlexContainer direction="column">
          <Text color="grey300">
            <FormattedMessage
              id="settings.integrationSettings.dbtCloudSettings.form.description"
              values={{
                lnk: (node: React.ReactNode) => (
                  <ExternalLink href={links.dbtCloudIntegrationDocs}>{node}</ExternalLink>
                ),
              }}
            />
          </Text>
          <Form<ServiceTokenFormValues>
            defaultValues={{ serviceToken: "" }}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
            schema={ServiceTokenFormSchema}
          >
            <FormControl
              name="serviceToken"
              fieldType="input"
              type="password"
              disabled={hasExistingToken}
              label={formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.form.serviceTokenLabel" })}
              placeholder={formatMessage({
                id: hasExistingToken
                  ? "settings.integrationSettings.dbtCloudSettings.form.serviceTokenAlreadyExist"
                  : "settings.integrationSettings.dbtCloudSettings.form.serviceTokenInputHidden",
              })}
            />
            {hasExistingToken ? (
              <FlexContainer justifyContent="flex-end">
                <Button variant="danger" type="button" onClick={onDeleteClick}>
                  <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.actions.delete" />
                </Button>
              </FlexContainer>
            ) : (
              <FormSubmissionButtons submitKey="form.saveChanges" />
            )}
          </Form>
        </FlexContainer>
      </Card>
    </Card>
  );
};
