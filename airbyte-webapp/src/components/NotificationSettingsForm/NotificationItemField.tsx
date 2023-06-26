import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FeatureItem, useFeature } from "core/services/features";

import { NotificationSettingsFormValues } from "./NotificationSettingsForm";
import { SlackNotificationUrlInput } from "./SlackNotificationUrlInput";
import { TestWebhookButton } from "./TestWebhookButton";

interface NotificationItemFieldProps {
  emailNotificationRequired?: boolean;
  name: keyof NotificationSettingsFormValues;
}

export const NotificationItemField: React.FC<NotificationItemFieldProps> = ({ emailNotificationRequired, name }) => {
  const emailNotificationsFeature = useFeature(FeatureItem.EmailNotifications);
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

  return (
    <>
      <div>
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
          {!emailNotificationRequired && <Switch onChange={onToggleEmailNotification} checked={field.customerio} />}
          {emailNotificationRequired && (
            <Tooltip control={<Switch checked disabled />}>
              <FormattedMessage id="settings.notifications.requiredNotificationTooltip" />
            </Tooltip>
          )}
        </FlexContainer>
      )}
      <FlexContainer justifyContent="center">
        <Switch onChange={onToggleSlackNotification} checked={field.slack} />
      </FlexContainer>
      <SlackNotificationUrlInput name={`${name}.slackWebhookLink`} disabled={!field.slack} />
      <TestWebhookButton disabled={!field.slack} webhookUrl={field.slackWebhookLink ?? ""} notificationTrigger={name} />
    </>
  );
};
