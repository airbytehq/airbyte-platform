import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import classnames from "classnames";
import React, { useCallback } from "react";
import { UseFormReturn } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Form } from "components/ui/forms";
import { ExternalLink } from "components/ui/Link";
import { ScrollParent } from "components/ui/ScrollParent";
import { Spinner } from "components/ui/Spinner";

import { ConnectionActionsBlock } from "area/connection/components/ConnectionActionsBlock";
import { FormConnectionFormValues, useInitialFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { useConnectionValidationZodSchema } from "area/connection/components/ConnectionForm/schemas/connectionSchema";
import { ConnectionSyncContextProvider } from "area/connection/components/ConnectionSync/ConnectionSyncContext";
import { I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED } from "area/connection/components/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionScheduleFormField";
import { SimplifiedConnectionsSettingsCard } from "area/connection/components/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";
import { useConnectionEditService } from "area/connection/utils/ConnectionEdit/ConnectionEditService";
import { HttpError, HttpProblem } from "core/api";
import { WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useNotificationService } from "core/services/Notification";
import { useFormMode } from "core/services/ui/FormModeContext";
import { useCheckSubHourlySchedule } from "core/utils/checkSubHourlySchedule";
import { trackError } from "core/utils/datadog";
import { useProFeaturesModal } from "core/utils/useProFeaturesModal";

import styles from "./ConnectionSettingsPage.module.scss";
import { StateBlock } from "./StateBlock";

export interface ConnectionSettingsFormValues {
  connectionName: string;
  geography?: string;
  notifySchemaChanges?: boolean;
}

export const ConnectionSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_SETTINGS);

  const { connection, updateConnection } = useConnectionEditService();
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { mode } = useFormMode();
  const simplifiedInitialValues = useInitialFormValues(connection, mode);
  const checkSubHourlySchedule = useCheckSubHourlySchedule();
  const { showProFeatureModalIfNeeded } = useProFeaturesModal("sub-hourly-sync");

  const zodValidationSchema = useConnectionValidationZodSchema();

  const onSubmit = useCallback(
    async (values: FormConnectionFormValues) => {
      const connectionUpdates: WebBackendConnectionUpdate = {
        connectionId: connection.connectionId,
        skipReset: true,
        ...values,
      };

      // Check for sub-hourly schedule and show modal if needed
      const isSubHourly = await checkSubHourlySchedule({
        scheduleType: values.scheduleType,
        scheduleData: values.scheduleData,
      });

      if (isSubHourly) {
        await showProFeatureModalIfNeeded();
      }

      return updateConnection(connectionUpdates);
    },
    [connection.connectionId, updateConnection, checkSubHourlySchedule, showProFeatureModalIfNeeded]
  );

  const onSuccess = useCallback(() => {
    registerNotification({
      id: "connection_settings_change_success",
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  }, [formatMessage, registerNotification]);

  const onError = useCallback(
    (error: Error, values: FormConnectionFormValues, methods: UseFormReturn<FormConnectionFormValues>) => {
      trackError(error, { connectionName: values.name });
      if (error instanceof HttpError && HttpProblem.isType(error, "error:cron-validation/under-one-hour-not-allowed")) {
        methods.setError("scheduleData.cron.cronExpression", {
          message: I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED,
        });
        return;
      }

      if (error instanceof HttpError && HttpProblem.isType(error, "error:connection-conflicting-destination-stream")) {
        registerNotification({
          id: "connection.conflictingDestinationStream",
          text: formatMessage(
            {
              id: "connectionForm.conflictingDestinationStream",
            },
            {
              stream: error.response?.data?.streams?.[0]?.streamName,
              moreCount:
                (error.response?.data?.streams?.length ?? 0) > 1 ? (error.response?.data?.streams?.length ?? 1) - 1 : 0,
              lnk: (...lnk: React.ReactNode[]) => (
                <ExternalLink href={error.response.documentationUrl ?? ""}>{lnk}</ExternalLink>
              ),
            }
          ),
          actionBtnText: formatMessage({ id: "connectionForm.conflictingDestinationStream.action" }),
          onAction: async () => {
            const randomPrefix = `${Math.random().toString(36).substring(2, 8)}_`;
            methods.setValue("prefix", randomPrefix);
            unregisterNotificationById("connection.conflictingDestinationStream");
            await methods.handleSubmit(onSubmit)();
            onSuccess();
          },
          type: "error",
        });
        return;
      }

      registerNotification({
        id: "connection_settings_change_error",
        text: formatMessage({ id: "connection.updateFailed" }),
        type: "error",
      });
    },
    [formatMessage, onSubmit, registerNotification, unregisterNotificationById, onSuccess]
  );

  const isDeprecated = connection.status === "deprecated";
  const hasConfiguredGeography = false;

  return (
    <ScrollParent>
      <FlexContainer direction="column">
        <Form<FormConnectionFormValues>
          trackDirtyChanges
          disabled={mode === "readonly"}
          onSubmit={onSubmit}
          onSuccess={onSuccess}
          onError={onError}
          zodSchema={zodValidationSchema}
          defaultValues={simplifiedInitialValues}
          reinitializeDefaultValues
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
