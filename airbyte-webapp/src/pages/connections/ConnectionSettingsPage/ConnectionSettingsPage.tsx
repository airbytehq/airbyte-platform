import { Disclosure } from "@headlessui/react";
import classnames from "classnames";
import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { ConnectionDangerBlock } from "components/common/ConnectionDangerBlock";
import { ConnectionDeleteBlock } from "components/common/ConnectionDeleteBlock";
import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
  useInitialFormValues,
} from "components/connection/ConnectionForm/formConfig";
import { SimplifiedConnectionsSettingsCard } from "components/connection/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";
import { Form } from "components/forms";
import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Spinner } from "components/ui/Spinner";

import {
  useCurrentWorkspace,
  useDeleteConnection,
  useDestinationDefinitionVersion,
  useResetConnection,
} from "core/api";
import { Geography, WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";
import { useIntent } from "core/utils/rbac";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
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
  const canUpdateDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const canSendSchemaUpdateNotifications = useFeature(FeatureItem.AllowAutoDetectSchema);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const { trackError } = useAppMonitoringService();
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);

  const { workspaceId } = useCurrentWorkspace();
  const canEditConnection = useIntent("EditConnection", { workspaceId });

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

  const isSimplifiedCreation = useExperiment("connection.simplifiedCreation", false);

  if (isSimplifiedCreation) {
    return <SimplifiedConnectionSettingsPage />;
  }

  return (
    <div className={styles.container}>
      <FlexContainer direction="column" justifyContent="flex-start">
        <Card>
          <Heading as="h2" size="sm" className={styles.heading}>
            <FormattedMessage id="connectionForm.connectionSettings" />
          </Heading>
          <Form<ConnectionSettingsFormValues>
            trackDirtyChanges
            disabled={!canEditConnection}
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
        {connection.status !== "deprecated" && <ConnectionDeleteBlock />}
      </FlexContainer>
      <Disclosure>
        {({ open }) => (
          <>
            <Disclosure.Button
              as={Button}
              variant="clear"
              icon={open ? "chevronDown" : "chevronRight"}
              className={styles.advancedButton}
            >
              <FormattedMessage id="connectionForm.settings.advancedButton" />
            </Disclosure.Button>
            <Disclosure.Panel className={styles.advancedPanel}>
              <React.Suspense fallback={<Spinner />}>
                <StateBlock connectionId={connection.connectionId} disabled={mode === "readonly"} />
              </React.Suspense>
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </div>
  );
};

const SimplifiedConnectionSettingsPage = () => {
  const { connection, updateConnection } = useConnectionEditService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);

  const { mode } = useConnectionFormService();
  const destDefinitionVersion = useDestinationDefinitionVersion(connection.destinationId);
  const simplifiedInitialValues = useInitialFormValues(connection, destDefinitionVersion, mode === "edit");

  const { workspaceId } = useCurrentWorkspace();
  const canEditConnection = useIntent("EditConnection", { workspaceId });

  const validationSchema = useConnectionValidationSchema();

  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const onDelete = () => deleteConnection(connection);

  const { mutateAsync: doResetConnection } = useResetConnection();
  const onReset = useCallback(async () => {
    await doResetConnection(connection.connectionId);
    registerNotification({
      id: sayClearInsteadOfReset ? "clearData.successfulStart" : "connection_reset_start_success",
      text: formatMessage({
        id: sayClearInsteadOfReset ? "form.clearData.successfulStart" : "form.resetData.successfulStart",
      }),
      type: "success",
    });
  }, [doResetConnection, connection.connectionId, registerNotification, sayClearInsteadOfReset, formatMessage]);

  const onSuccess = () => {
    registerNotification({
      id: "connection_settings_change_success",
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: FormConnectionFormValues) => {
    trackError(e, { connectionName: name });
    registerNotification({
      id: "connection_settings_change_error",
      text: formatMessage({ id: "connection.updateFailed" }),
      type: "error",
    });
  };

  const isDeprecated = connection.status === "deprecated";

  return (
    <FlexContainer direction="column">
      <Form<FormConnectionFormValues>
        trackDirtyChanges
        disabled={!canEditConnection}
        onSubmit={(values: FormConnectionFormValues) => {
          const connectionUpdates: WebBackendConnectionUpdate = {
            connectionId: connection.connectionId,
            ...values,
          };

          return updateConnection(connectionUpdates);
        }}
        onError={onError}
        onSuccess={onSuccess}
        schema={validationSchema}
        defaultValues={simplifiedInitialValues}
      >
        <SimplifiedConnectionsSettingsCard
          title={formatMessage({ id: "sources.settings" })}
          source={connection.source}
          destination={connection.destination}
          isCreating={false}
          isDeprecated={isDeprecated}
        />
      </Form>

      {connection.status !== "deprecated" && <ConnectionDangerBlock onDelete={onDelete} onReset={onReset} />}

      <Disclosure>
        {({ open }) => (
          <>
            <Disclosure.Button
              as={Button}
              variant="clear"
              icon={open ? "chevronDown" : "chevronRight"}
              iconPosition="right"
              className={classnames(styles.advancedButton, styles.alignStart)}
            >
              <FormattedMessage id="connection.state.title" />
            </Disclosure.Button>
            <Disclosure.Panel className={styles.advancedPanel}>
              <React.Suspense fallback={<Spinner />}>
                <StateBlock connectionId={connection.connectionId} disabled={mode === "readonly"} />
              </React.Suspense>
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    </FlexContainer>
  );
};
