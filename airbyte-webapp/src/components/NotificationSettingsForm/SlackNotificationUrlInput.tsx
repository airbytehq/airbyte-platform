import { Path, get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { NotificationSettingsFormValues } from "./NotificationSettingsForm";
import styles from "./SlackNotificationUrlInput.module.scss";

interface SlackNotificationUrlInputProps {
  name: Path<NotificationSettingsFormValues>;
  disabled?: boolean;
}

export const SlackNotificationUrlInput: React.FC<SlackNotificationUrlInputProps> = ({ name, disabled = false }) => {
  const { register } = useFormContext<NotificationSettingsFormValues>();
  const { errors } = useFormState<NotificationSettingsFormValues>({ name });
  const { formatMessage } = useIntl();
  const error = get(errors, name);

  return (
    <div className={styles.webhookInput}>
      <Input
        {...register(name)}
        error={!!error}
        disabled={disabled}
        placeholder={formatMessage({ id: "settings.notifications.webhookUrl" })}
      />
      {error?.message && (
        <Text color="red" className={styles.webhookInput__errorMessage}>
          <FormattedMessage id={error.message} />
        </Text>
      )}
    </div>
  );
};
