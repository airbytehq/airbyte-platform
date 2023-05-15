import classNames from "classnames";
import { Field, FieldProps, Form, Formik, useFormikContext } from "formik";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { SecretTextArea } from "components/ui/SecretTextArea";
import { Text } from "components/ui/Text";

import { useNotificationService } from "hooks/services/Notification";
import { useDbtCloudServiceToken } from "packages/cloud/services/dbtCloud";
import { SettingsCard } from "pages/SettingsPage/pages/SettingsComponents";
import { links } from "utils/links";

import styles from "./DbtCloudSettingsView.module.scss";
import { useDbtTokenRemovalModal } from "./useDbtTokenRemovalModal";
interface ServiceTokenFormValues {
  serviceToken: string;
}

export const cleanedErrorMessage = (e: Error): string => e.message.replace("Internal Server Error: ", "");
// a centrally-defined key for accessing the token value within formik objects

export const DbtCloudSettingsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const { hasExistingToken, saveToken, isSavingToken, isDeletingToken } = useDbtCloudServiceToken();
  const [hasValidationError, setHasValidationError] = useState(false);
  const { registerNotification } = useNotificationService();

  const onDeleteClick = useDbtTokenRemovalModal();

  const ButtonGroup = () => {
    const { resetForm, values } = useFormikContext<ServiceTokenFormValues>();

    return (
      <div className={classNames(styles.controlGroup, styles.formButtons)}>
        {hasExistingToken && (
          <Button
            variant="danger"
            type="button"
            className={classNames(styles.button, styles.deleteButton)}
            onClick={onDeleteClick}
            isLoading={isDeletingToken}
          >
            <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.actions.delete" />
          </Button>
        )}

        <div className={styles.editActionButtons}>
          <Button
            variant="secondary"
            type="button"
            disabled={!values.serviceToken}
            onClick={() => {
              resetForm();
            }}
          >
            <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.actions.cancel" />
          </Button>
          <Button
            variant="primary"
            type="submit"
            disabled={!values.serviceToken}
            className={styles.button}
            isLoading={isSavingToken}
          >
            <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.actions.submit" />
          </Button>
        </div>
      </div>
    );
  };

  const ServiceTokenForm = () => (
    <Formik
      initialValues={{
        serviceToken: "",
      }}
      onSubmit={({ serviceToken }, { resetForm }) => {
        setHasValidationError(false);
        return saveToken(serviceToken, {
          onError: (e) => {
            setHasValidationError(true);
            registerNotification({
              id: "dbtCloud/save-token-failure",
              text: cleanedErrorMessage(e),
              type: "error",
            });
          },
          onSuccess: () => {
            registerNotification({
              id: "dbtCloud/save-token-success",
              text: formatMessage({ id: "settings.integrationSettings.dbtCloudSettings.actions.submit.success" }),
              type: "success",
            });
            resetForm();
          },
        });
      }}
    >
      <Form>
        <label htmlFor="serviceToken">
          <FormattedMessage id="settings.integrationSettings.dbtCloudSettings.form.serviceTokenLabel" />
          <Field name="serviceToken">
            {({ field }: FieldProps<string>) => (
              <SecretTextArea
                {...field}
                hiddenMessage={formatMessage({
                  id: "settings.integrationSettings.dbtCloudSettings.form.serviceTokenInputHidden",
                })}
                error={hasValidationError}
                leftJustified
                hiddenWhenEmpty
              />
            )}
          </Field>
        </label>
        <ButtonGroup />
      </Form>
    </Formik>
  );

  return (
    <SettingsCard title={<FormattedMessage id="settings.integrationSettings.dbtCloudSettings" />}>
      <div className={styles.cardContent}>
        <Text className={styles.description}>
          <FormattedMessage
            id="settings.integrationSettings.dbtCloudSettings.form.description"
            values={{
              lnk: (node: React.ReactNode) => (
                <a href={links.dbtCloudIntegrationDocs} target="_blank" rel="noreferrer">
                  {node}
                </a>
              ),
            }}
          />
        </Text>
        <ServiceTokenForm />
      </div>
    </SettingsCard>
  );
};
