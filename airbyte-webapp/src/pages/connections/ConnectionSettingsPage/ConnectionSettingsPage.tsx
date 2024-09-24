import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import classnames from "classnames";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
  useInitialFormValues,
} from "components/connection/ConnectionForm/formConfig";
import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { SimplifiedConnectionsSettingsCard } from "components/connection/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";
import { Form } from "components/forms";
import { ScrollableContainer } from "components/ScrollableContainer";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";

import { ConnectionActionsBlock } from "area/connection/components/ConnectionActionsBlock";
import { useCurrentWorkspace } from "core/api";
import { Geography, WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { useIntent } from "core/utils/rbac";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./ConnectionSettingsPage.module.scss";
import { StateBlock } from "./StateBlock";

export interface ConnectionSettingsFormValues {
  connectionName: string;
  geography?: Geography;
  notifySchemaChanges?: boolean;
}

export const ConnectionSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);

  const { connection, updateConnection } = useConnectionEditService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  const { mode } = useConnectionFormService();
  const simplifiedInitialValues = useInitialFormValues(connection, mode);

  const { workspaceId } = useCurrentWorkspace();
  const canEditConnection = useIntent("EditConnection", { workspaceId });

  const validationSchema = useConnectionValidationSchema();

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
    <ScrollableContainer>
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

        {connection.status !== "deprecated" && (
          <ConnectionSyncContextProvider>
            <ConnectionActionsBlock />
          </ConnectionSyncContextProvider>
        )}

        <Disclosure>
          {({ open }) => (
            <>
              <DisclosureButton
                as={Button}
                variant="clear"
                icon={open ? "chevronDown" : "chevronRight"}
                iconPosition="right"
                className={classnames(styles.advancedButton, styles.alignStart)}
              >
                <FormattedMessage id="connection.state.title" />
              </DisclosureButton>
              <DisclosurePanel className={styles.advancedPanel}>
                <React.Suspense fallback={<Spinner />}>
                  <StateBlock connectionId={connection.connectionId} disabled={mode === "readonly"} />
                </React.Suspense>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      </FlexContainer>
    </ScrollableContainer>
  );
};
