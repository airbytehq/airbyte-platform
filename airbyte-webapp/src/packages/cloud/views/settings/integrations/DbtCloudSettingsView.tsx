import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useDbtCloudServiceToken } from "core/api/cloud";
import { useFormatError } from "core/errors";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

import { useDbtTokenRemovalModal } from "./useDbtTokenRemovalModal";

const ServiceTokenFormSchema = z.object({
  authToken: z.string().trim().nonempty("form.empty.error"),
  accessUrl: z.string().trim().optional(),
});

type DbtConfigurationFormValues = z.infer<typeof ServiceTokenFormSchema>;

export const DbtCloudSettingsView: React.FC = () => {
  const formatError = useFormatError();
  const { formatMessage } = useIntl();
  const { hasExistingToken, saveToken } = useDbtCloudServiceToken();
  const { registerNotification } = useNotificationService();
  const canUpdateWorkspace = useGeneratedIntent(Intent.UpdateWorkspace);

  const onDeleteClick = useDbtTokenRemovalModal();

  const onSubmit = async (values: DbtConfigurationFormValues) => {
    await saveToken({
      authToken: values.authToken,
      accessUrl: values.accessUrl || undefined,
    });
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
      text: formatError(e),
      type: "error",
    });
  };

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.integrationSettings.dbtCloudSettings" })}</Heading>

      <>
        {hasExistingToken ? (
          <>
            <Message
              text={
                <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.form.serviceTokenAlreadyExist" />
              }
            />
            <FlexContainer justifyContent="flex-start">
              <Button variant="danger" type="button" onClick={onDeleteClick}>
                <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.actions.delete" />
              </Button>
            </FlexContainer>
          </>
        ) : (
          <>
            <Text>
              <FormattedMessage
                id="settings.integrationSettings.dbtCloudSettings.form.description"
                values={{
                  lnk: (node: React.ReactNode) => (
                    <ExternalLink href={links.dbtCloudIntegrationDocs}>{node}</ExternalLink>
                  ),
                }}
              />
            </Text>

            <Form<DbtConfigurationFormValues>
              defaultValues={{ authToken: "" }}
              onSubmit={onSubmit}
              onSuccess={onSuccess}
              onError={onError}
              zodSchema={ServiceTokenFormSchema}
              disabled={!canUpdateWorkspace}
            >
              <FormControl
                name="accessUrl"
                fieldType="input"
                optional
                disabled={hasExistingToken}
                label={formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.form.urlLabel" })}
                placeholder={formatMessage({
                  id: "settings.integrationSettings.dbtCloudSettings.form.urlPlaceholder",
                })}
              />
              <FormControl
                name="authToken"
                fieldType="input"
                type="password"
                disabled={hasExistingToken}
                label={formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.form.serviceTokenLabel" })}
                placeholder={formatMessage({
                  id: hasExistingToken
                    ? "general.maskedString"
                    : "settings.integrationSettings.dbtCloudSettings.form.serviceTokenInputHidden",
                })}
              />

              <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />
            </Form>
          </>
        )}
      </>
    </FlexContainer>
  );
};
