import { Disclosure } from "@headlessui/react";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { DeleteBlock } from "components/common/DeleteBlock";
import { Form } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Spinner } from "components/ui/Spinner";

import { useDeleteConnection } from "core/api";
import { Geography, WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./ConnectionSettingsPage.module.scss";
import { SchemaUpdateNotifications } from "./SchemaUpdateNotifications";
import { StateBlock } from "./StateBlock";
import { UpdateConnectionName } from "./UpdateConnectionName";

export interface ConnectionSettingsFormValues {
  connectionName: string;
  geography?: Geography;
  notifySchemaChanges?: boolean;
}

const connectionSettingsFormSchema = yup.object({
  connectionName: yup.string().trim().required("form.empty.error"),
  geography: yup.mixed<Geography>().optional(),
  notifySchemaChanges: yup.bool().optional(),
});

const dataResidencyDropdownDescription = (
  <FormattedMessage
    id="connection.geographyDescription"
    values={{
      ipLink: (node: React.ReactNode) => <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>,
      docLink: (
        <ExternalLink href={links.connectionDataResidency}>
          <FormattedMessage id="ui.learnMore" />
        </ExternalLink>
      ),
    }}
  />
);

export const ConnectionSettingsPage: React.FC = () => {
  const { connection, updateConnection } = useConnectionEditService();
  const { mode } = useConnectionFormService();
  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const canUpdateDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const canSendSchemaUpdateNotifications = useFeature(FeatureItem.AllowAutoDetectSchema);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const { trackError } = useAppMonitoringService();
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);
  const onDelete = () => deleteConnection(connection);

  const onSuccess = () => {
    registerNotification({
      id: "connection_settings_change_success",
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  const onError = (e: Error, { connectionName }: ConnectionSettingsFormValues) => {
    trackError(e, { connectionName });
    registerNotification({
      id: "connection_settings_change_error",
      text: formatMessage({ id: "connection.updateFailed" }),
      type: "error",
    });
  };

  const connectionSettingsDefaultValues = () => {
    const defaultValues: ConnectionSettingsFormValues = {
      connectionName: connection.name,
    };
    if (canSendSchemaUpdateNotifications) {
      defaultValues.notifySchemaChanges = connection.notifySchemaChanges;
    }
    if (canUpdateDataResidency) {
      defaultValues.geography = connection.geography;
    }
    return defaultValues;
  };

  return (
    <div className={styles.container}>
      <FlexContainer direction="column" justifyContent="flex-start">
        <Card withPadding>
          <Heading as="h2" size="sm" className={styles.heading}>
            <FormattedMessage id="connectionForm.connectionSettings" />
          </Heading>
          <Form<ConnectionSettingsFormValues>
            trackDirtyChanges
            onSubmit={({ connectionName, geography, notifySchemaChanges }) => {
              const connectionUpdates: WebBackendConnectionUpdate = {
                name: connectionName,
                connectionId: connection.connectionId,
              };

              if (canUpdateDataResidency) {
                connectionUpdates.geography = geography;
              }

              if (canSendSchemaUpdateNotifications) {
                connectionUpdates.notifySchemaChanges = notifySchemaChanges;
              }

              return updateConnection(connectionUpdates);
            }}
            onError={onError}
            onSuccess={onSuccess}
            schema={connectionSettingsFormSchema}
            defaultValues={connectionSettingsDefaultValues()}
          >
            <UpdateConnectionName />
            {canSendSchemaUpdateNotifications && <SchemaUpdateNotifications disabled={mode === "readonly"} />}
            {canUpdateDataResidency && (
              <DataResidencyDropdown<ConnectionSettingsFormValues>
                labelId="connection.geographyTitle"
                description={dataResidencyDropdownDescription}
                name="geography"
                disabled={mode === "readonly"}
              />
            )}
            <FormSubmissionButtons submitKey="form.saveChanges" />
          </Form>
        </Card>
        {connection.status !== "deprecated" && <DeleteBlock type="connection" onDelete={onDelete} />}
      </FlexContainer>
      <Disclosure>
        {({ open }) => (
          <>
            <Disclosure.Button
              as={Button}
              variant="clear"
              icon={<Icon type={open ? "chevronDown" : "chevronRight"} />}
              className={styles.advancedButton}
            >
              <FormattedMessage id="connectionForm.settings.advancedButton" />
            </Disclosure.Button>
            <Disclosure.Panel className={styles.advancedPanel}>
              <React.Suspense fallback={<Spinner />}>
                <StateBlock connectionId={connection.connectionId} syncCatalog={connection.syncCatalog} />
              </React.Suspense>
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </div>
  );
};
