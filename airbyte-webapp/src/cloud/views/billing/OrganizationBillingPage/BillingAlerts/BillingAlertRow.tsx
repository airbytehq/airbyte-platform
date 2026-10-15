import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { FormControl } from "components/ui/forms";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import styles from "./BillingAlertRow.module.scss";
import { BillingAlertsFormValues } from "./billingAlertsForm";

interface BillingAlertRowProps {
  enabledName: "syncAlertEnabled" | "invoiceAlertEnabled";
  thresholdName: "syncCreditThreshold" | "invoiceAmountThreshold";
  placeholder: string;
  messageIdPrefix:
    | "settings.organization.billing.alerts.syncAlert"
    | "settings.organization.billing.alerts.invoiceAlert";
}

export const BillingAlertRow: React.FC<BillingAlertRowProps> = ({
  enabledName,
  thresholdName,
  placeholder,
  messageIdPrefix,
}) => {
  const { formatMessage } = useIntl();
  const { control, setValue, getFieldState, formState } = useFormContext<BillingAlertsFormValues>();
  const enabled = useWatch({ control, name: enabledName });
  const { error: thresholdError } = getFieldState(thresholdName, formState);
  const switchId = `billing-alerts-${enabledName}`;
  const thresholdErrorId = `billing-alerts-${thresholdName}-error`;

  return (
    <FlexContainer direction="column" gap="sm">
      <FlexContainer alignItems="center" gap="lg">
        <FormControl<BillingAlertsFormValues>
          name={enabledName}
          fieldType="switch"
          id={switchId}
          reserveSpaceForError={false}
          onChange={(event) => {
            // An alert that is off is stored as 0, so its input is emptied rather than kept hidden.
            if (!event.target.checked) {
              setValue(thresholdName, "", { shouldDirty: true, shouldValidate: true });
            }
          }}
        />
        <Text as="span">
          <label htmlFor={switchId}>{formatMessage({ id: `${messageIdPrefix}.label` })}</label>
        </Text>
        <Controller
          control={control}
          name={thresholdName}
          render={({ field, fieldState }) => (
            <Input
              {...field}
              containerClassName={styles.thresholdInput}
              inputMode="numeric"
              disabled={!enabled}
              placeholder={placeholder}
              error={!!fieldState.error}
              aria-label={formatMessage({ id: `${messageIdPrefix}.thresholdLabel` })}
              aria-describedby={thresholdError ? thresholdErrorId : undefined}
            />
          )}
        />
        <Text as="span">
          <FormattedMessage id={`${messageIdPrefix}.unit`} />
        </Text>
      </FlexContainer>
      {thresholdError?.message && (
        <Text size="xs" color="red" id={thresholdErrorId}>
          <FormattedMessage id={thresholdError.message} />
        </Text>
      )}
      <Text size="sm" color="grey">
        <FormattedMessage id={`${messageIdPrefix}.helper`} />
      </Text>
    </FlexContainer>
  );
};
