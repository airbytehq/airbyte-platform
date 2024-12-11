import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import classnames from "classnames";
import React, { useCallback } from "react";
import { UseFormReturn } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
  useInitialFormValues,
} from "components/connection/ConnectionForm/formConfig";
import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED } from "components/connection/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionScheduleFormField";
import { SimplifiedConnectionsSettingsCard } from "components/connection/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";
import { Form } from "components/forms";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ScrollParent } from "components/ui/ScrollParent";
import { Spinner } from "components/ui/Spinner";

import { ConnectionActionsBlock } from "area/connection/components/ConnectionActionsBlock";
import { HttpError, HttpProblem, useCurrentWorkspace } from "core/api";
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
  const { defaultGeography } = useCurrentWorkspace();
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

  const onError = useCallback(
    (error: Error, values: FormConnectionFormValues, methods: UseFormReturn<FormConnectionFormValues>) => {
      trackError(error, { connectionName: values.name });
      if (error instanceof HttpError && HttpProblem.isType(error, "error:cron-validation/under-one-hour-not-allowed")) {
        methods.setError("scheduleData.cron.cronExpression", {
          message: I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED,
        });
      }
      registerNotification({
        id: "connection_settings_change_error",
        text: formatMessage({ id: "connection.updateFailed" }),
        type: "error",
      });
    },
    [formatMessage, registerNotification]
  );

  const isDeprecated = connection.status === "deprecated";
  const hasConfiguredGeography =
    connection.geography !== undefined &&
    connection.geography !== defaultGeography &&
    connection.geography !== Geography.auto;

  return (
    <ScrollParent>
      <FlexContainer direction="column">
        <Form<FormConnectionFormValues>
          trackDirtyChanges
          disabled={!canEditConnection}
          onSubmit={(values: FormConnectionFormValues) => {
            const connectionUpdates: WebBackendConnectionUpdate = {
              connectionId: connection.connectionId,
              skipReset: true,
              ...values,
            };

            return updateConnection(connectionUpdates);
          }}
          onSuccess={onSuccess}
          onError={onError}
          schema={validationSchema}
          defaultValues={simplifiedInitialValues}
        >
          <SimplifiedConnectionsSettingsCard
            title={formatMessage({ id: "sources.settings" })}
            source={connection.source}
            destination={connection.destination}
            isCreating={false}
            isDeprecated={isDeprecated}
            hasConfiguredGeography={hasConfiguredGeography}
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
    </ScrollParent>
  );
};
