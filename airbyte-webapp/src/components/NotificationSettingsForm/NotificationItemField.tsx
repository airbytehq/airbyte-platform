import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FeatureItem, useFeature } from "core/services/features";

import styles from "./NotificationItemField.module.scss";
import { NotificationSettingsFormValues } from "./NotificationSettingsForm";
import { SlackNotificationUrlInput } from "./SlackNotificationUrlInput";
import { TestWebhookButton } from "./TestWebhookButton";

interface NotificationItemFieldProps {
  emailNotificationRequired?: boolean;
  slackNotificationUnsupported?: boolean;
  name: keyof NotificationSettingsFormValues;
}

export const NotificationItemField: React.FC<NotificationItemFieldProps> = ({
  emailNotificationRequired,
  slackNotificationUnsupported,
  name,
}) => {
  const emailNotificationsFeature = useFeature(FeatureItem.EmailNotifications);
  const atLeastOneNotificationTypeEnableable = emailNotificationsFeature || !slackNotificationUnsupported;
  const { setValue, trigger } = useFormContext<NotificationSettingsFormValues>();
  const field = useWatch<NotificationSettingsFormValues, keyof NotificationSettingsFormValues>({ name });

  const onToggleEmailNotification = () => {
    setValue(`${name}.customerio`, !field.customerio, { shouldTouch: true, shouldDirty: true });
  };

  const onToggleSlackNotification = () => {
    setValue(`${name}.slack`, !field.slack, { shouldTouch: true, shouldDirty: true });

    // If disabling the webhook notification, the input will be disabled, so we need to
    // manually trigger validation to clear any potential validation errors being shown
    if (field.slack) {
      trigger(`${name}.slackWebhookLink`);
    }
  };

  if (!atLeastOneNotificationTypeEnableable) {
    return null;
  }

  return (
    <>
      <div className={styles.notificationItemField}>
        <FlexContainer direction="column" gap="xs">
          <Text>
            <FormattedMessage id={`settings.notifications.${name}`} />
          </Text>
          <Text color="grey">
            <FormattedMessage id={`settings.notifications.${name}.description`} />
          </Text>
        </FlexContainer>
      </div>

      {emailNotificationsFeature && (
        <FlexContainer justifyContent="center">
          {!emailNotificationRequired && (
            <Switch onChange={onToggleEmailNotification} checked={field.customerio} data-testid={`${name}.email`} />
          )}
          {emailNotificationRequired && (
            <Tooltip control={<Switch checked disabled />}>
              <FormattedMessage id="settings.notifications.requiredNotificationTooltip" />
            </Tooltip>
          )}
        </FlexContainer>
      )}

      {!slackNotificationUnsupported && (
        <>
          <FlexContainer justifyContent="center">
            <Switch onChange={onToggleSlackNotification} checked={field.slack} data-testid={`${name}.slack`} />
          </FlexContainer>
          <SlackNotificationUrlInput name={`${name}.slackWebhookLink`} disabled={!field.slack} />
          <TestWebhookButton
            disabled={!field.slack}
            webhookUrl={field.slackWebhookLink ?? ""}
            notificationTrigger={name}
          />
        </>
      )}
    </>
  );
};
