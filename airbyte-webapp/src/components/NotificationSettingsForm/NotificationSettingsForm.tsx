import { yupResolver } from "@hookform/resolvers/yup";
import classNames from "classnames";
import React from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { FormDevTools } from "components/forms/FormDevTools";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useTryNotificationWebhook } from "core/api";
import { NotificationReadStatus, NotificationSettings, NotificationTrigger } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { isFulfilled } from "core/utils/promises";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";
import { useUpdateNotificationSettings } from "hooks/services/useWorkspace";

import { formValuesToNotificationSettings } from "./formValuesToNotificationSettings";
import { NotificationItemField } from "./NotificationItemField";
import styles from "./NotificationSettingsForm.module.scss";
import { notificationSettingsToFormValues } from "./notificationSettingsToFormValues";

export const NotificationSettingsForm: React.FC = () => {
  const emailNotificationsFeatureEnabled = useFeature(FeatureItem.EmailNotifications);
  const breakingChangeNotificationsExperimentEnabled = useExperiment("settings.breakingChangeNotifications", false);
  const updateNotificationSettings = useUpdateNotificationSettings();
  const { notificationSettings } = useCurrentWorkspace();
  const defaultValues = notificationSettingsToFormValues(notificationSettings);
  const testWebhook = useTryNotificationWebhook();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const methods = useForm<NotificationSettingsFormValues>({
    defaultValues,
    reValidateMode: "onChange",
    mode: "onBlur",
    resolver: yupResolver(validationSchema),
  });

  const onSubmit = async (values: NotificationSettingsFormValues) => {
    // Additional validation of each webhook is done here to avoid spamming the API during the normal form validation cycle
    const webhookErrors = (
      await Promise.allSettled(
        notificationKeys.map(async (key) => {
          const notification = values[key];

          // If Slack is not set as a notification type, or if the webhook has not changed, we can skip the validation
          if (
            !notification.slack ||
            (!methods.formState.dirtyFields[key]?.slack && !methods.formState.dirtyFields[key]?.slackWebhookLink)
          ) {
            return { key, isValid: true };
          }

          // If the webhook is not set, we can assume it's not valid
          if (!notification.slackWebhookLink) {
            return { key, isValid: false };
          }

          // For all other webhook URLs, we need to validate them via our API
          const webhookValidation = await testWebhook({
            notificationTrigger: notificationTriggerMap[key],
            slackConfiguration: { webhook: notification.slackWebhookLink },
          }).catch(() => ({ status: NotificationReadStatus.failed }));
          return webhookValidation.status === NotificationReadStatus.succeeded
            ? { key, isValid: true }
            : { key, isValid: false };
        })
      )
    )
      .filter(isFulfilled)
      .filter(({ value }) => !value.isValid)
      .map(({ value }) => value.key);

    if (webhookErrors.length > 0) {
      // Display an error message for each invalid webhook
      webhookErrors.forEach((notificationKey) =>
        methods.setError(`${notificationKey}.slackWebhookLink`, {
          message: formatMessage({ id: "settings.webhook.test.failed" }),
        })
      );
    } else {
      // If there are no invalid webhooks we can actually update the workspace with the new notification settings
      await updateEmailNotifications(values);
    }
  };

  const updateEmailNotifications = async (values: NotificationSettingsFormValues) => {
    const newNotificationSettings = formValuesToNotificationSettings(values);
    try {
      await updateNotificationSettings(newNotificationSettings);
      methods.reset(values);
      registerNotification({
        id: "notification_settings_update",
        type: "success",
        text: formatMessage({ id: "settings.notifications.successMessage" }),
      });
    } catch (e) {
      trackError(e, {
        name: "notification_settings_update_error",
        formValues: values,
        requestPayload: newNotificationSettings,
      });
      registerNotification({
        id: "notification_settings_update",
        type: "error",
        text: formatMessage({ id: "settings.notifications.errorMessage" }),
      });
    }
  };

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)} data-testid="notification-settings-form">
        <FormDevTools />
        <Box mb="xl">
          <Text>
            <FormattedMessage id="settings.notifications.description" />
          </Text>
        </Box>
        <Box
          mb="md"
          className={classNames(styles.inputGrid, {
            [styles["inputGrid--withoutEmail"]]: !emailNotificationsFeatureEnabled,
          })}
        >
          <span />
          {emailNotificationsFeatureEnabled && (
            <Text align="center" color="grey">
              <FormattedMessage id="settings.notifications.email" />
            </Text>
          )}
          <Text align="center" color="grey">
            <FormattedMessage id="settings.notifications.webhook" />
          </Text>
          <Text color="grey">
            <FormattedMessage id="settings.notifications.webhookUrl" />
          </Text>
          <span />
        </Box>
        <div
          className={classNames(styles.inputGrid, {
            [styles["inputGrid--withoutEmail"]]: !emailNotificationsFeatureEnabled,
          })}
        >
          <NotificationItemField name="sendOnFailure" />
          <NotificationItemField name="sendOnSuccess" />
          <NotificationItemField name="sendOnConnectionUpdate" />
          <NotificationItemField name="sendOnConnectionUpdateActionRequired" emailNotificationRequired />
          <NotificationItemField name="sendOnSyncDisabledWarning" />
          <NotificationItemField name="sendOnSyncDisabled" emailNotificationRequired />
          {breakingChangeNotificationsExperimentEnabled && (
            <>
              <NotificationItemField name="sendOnBreakingChangeWarning" slackNotificationUnsupported />
              <NotificationItemField
                name="sendOnBreakingChangeSyncsDisabled"
                emailNotificationRequired
                slackNotificationUnsupported
              />
            </>
          )}
        </div>
        <Box mt="lg">
          <FormSubmissionButtons submitKey="form.saveChanges" />
        </Box>
      </form>
    </FormProvider>
  );
};

export interface NotificationItemFieldValue {
  slack: boolean;
  customerio: boolean;
  slackWebhookLink?: string;
}

export interface NotificationSettingsFormValues {
  sendOnFailure: NotificationItemFieldValue;
  sendOnSuccess: NotificationItemFieldValue;
  sendOnConnectionUpdate: NotificationItemFieldValue;
  sendOnConnectionUpdateActionRequired: NotificationItemFieldValue;
  sendOnSyncDisabled: NotificationItemFieldValue;
  sendOnSyncDisabledWarning: NotificationItemFieldValue;
  sendOnBreakingChangeWarning: NotificationItemFieldValue;
  sendOnBreakingChangeSyncsDisabled: NotificationItemFieldValue;
}

const notificationItemSchema: SchemaOf<NotificationItemFieldValue> = yup.object({
  slack: yup.boolean().required(),
  customerio: yup.boolean().required(),
  slackWebhookLink: yup.string().when("slack", {
    is: true,
    then: yup.string().required("form.empty.error"),
  }),
});

const validationSchema: SchemaOf<NotificationSettingsFormValues> = yup.object({
  sendOnFailure: notificationItemSchema,
  sendOnSuccess: notificationItemSchema,
  sendOnConnectionUpdate: notificationItemSchema,
  sendOnConnectionUpdateActionRequired: notificationItemSchema,
  sendOnSyncDisabled: notificationItemSchema,
  sendOnSyncDisabledWarning: notificationItemSchema,
  sendOnBreakingChangeWarning: notificationItemSchema,
  sendOnBreakingChangeSyncsDisabled: notificationItemSchema,
});

export const notificationKeys: Array<keyof NotificationSettings> = [
  "sendOnFailure",
  "sendOnSuccess",
  "sendOnConnectionUpdate",
  "sendOnConnectionUpdateActionRequired",
  "sendOnSyncDisabled",
  "sendOnSyncDisabledWarning",
  "sendOnBreakingChangeWarning",
  "sendOnBreakingChangeSyncsDisabled",
];

export const notificationTriggerMap: Record<keyof NotificationSettings, NotificationTrigger> = {
  sendOnFailure: NotificationTrigger.sync_failure,
  sendOnSuccess: NotificationTrigger.sync_success,
  sendOnConnectionUpdate: NotificationTrigger.connection_update,
  sendOnConnectionUpdateActionRequired: NotificationTrigger.connection_update_action_required,
  sendOnSyncDisabled: NotificationTrigger.sync_disabled,
  sendOnSyncDisabledWarning: NotificationTrigger.sync_disabled_warning,
  sendOnBreakingChangeWarning: NotificationTrigger.breaking_change_warning,
  sendOnBreakingChangeSyncsDisabled: NotificationTrigger.breaking_change_syncs_disabled,
};
