import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer } from "components/ui/Flex";
import { Form, FormControl } from "components/ui/forms";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import {
  useGetBillingNotificationSettings,
  useGetPaymentInformation,
  useUpdateBillingNotificationSettings,
} from "core/api";
import { BillingNotificationSettingsRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useNotificationService } from "core/services/Notification";

import { BillingAlertRow } from "./BillingAlertRow";
import styles from "./BillingAlerts.module.scss";
import { BillingAlertsFormValues, billingAlertsFormSchema, toFormValues, toUpdateRequest } from "./billingAlertsForm";

const SYNC_CREDIT_THRESHOLD_PLACEHOLDER = "100";
const INVOICE_AMOUNT_THRESHOLD_PLACEHOLDER = "2000";

const BillingAlertsForm: React.FC<{ settings: BillingNotificationSettingsRead }> = ({ settings }) => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const { registerNotification } = useNotificationService();
  const formatError = useFormatError();
  const { mutateAsync: updateSettings } = useUpdateBillingNotificationSettings(organizationId);
  const { data: paymentInformation } = useGetPaymentInformation(organizationId);
  const billingEmail = paymentInformation?.customer?.email;
  const defaultValues = useMemo(() => toFormValues(settings), [settings]);

  return (
    <Form<BillingAlertsFormValues>
      defaultValues={defaultValues}
      // A revisit renders cached settings first; the background refetch must update an untouched form.
      reinitializeDefaultValues
      zodSchema={billingAlertsFormSchema}
      onSubmit={async (values) => {
        const savedSettings = await updateSettings(toUpdateRequest(values));
        return { resetValues: toFormValues(savedSettings) };
      }}
      onSuccess={() =>
        registerNotification({
          id: "billingAlertsSaved",
          type: "success",
          text: <FormattedMessage id="settings.organization.billing.alerts.saveSuccess" />,
        })
      }
      onError={(error) =>
        registerNotification({
          id: "billingAlertsSaveError",
          type: "error",
          text: formatError(error),
        })
      }
    >
      <FlexContainer direction="column" gap="xl">
        <FormControl<BillingAlertsFormValues>
          containerClassName={styles.emailInput}
          name="email"
          fieldType="input"
          type="email"
          autoComplete="email"
          label={formatMessage({ id: "settings.organization.billing.alerts.email.label" })}
          placeholder={billingEmail || formatMessage({ id: "settings.organization.billing.alerts.email.placeholder" })}
          footer={formatMessage({ id: "settings.organization.billing.alerts.email.helper" })}
          reserveSpaceForError={false}
        />
        <BillingAlertRow
          enabledName="syncAlertEnabled"
          thresholdName="syncCreditThreshold"
          placeholder={SYNC_CREDIT_THRESHOLD_PLACEHOLDER}
          messageIdPrefix="settings.organization.billing.alerts.syncAlert"
        />
        <BillingAlertRow
          enabledName="invoiceAlertEnabled"
          thresholdName="invoiceAmountThreshold"
          placeholder={INVOICE_AMOUNT_THRESHOLD_PLACEHOLDER}
          messageIdPrefix="settings.organization.billing.alerts.invoiceAlert"
        />
        <FormSubmissionButtons submitKey="form.saveChanges" allowNonDirtyCancel />
      </FlexContainer>
    </Form>
  );
};

export const BillingAlerts: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const { data: settings, isError } = useGetBillingNotificationSettings(organizationId);

  return (
    <FlexContainer direction="column" gap="lg">
      <FlexContainer direction="column" gap="xs">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.alerts.title" />
        </Heading>
        <Text size="sm" color="grey">
          <FormattedMessage id="settings.organization.billing.alerts.description" />
        </Text>
      </FlexContainer>
      {settings ? (
        <BillingAlertsForm settings={settings} />
      ) : isError ? (
        <DataLoadingError>
          <FormattedMessage id="settings.organization.billing.alerts.loadError" />
        </DataLoadingError>
      ) : (
        <LoadingSkeleton />
      )}
    </FlexContainer>
  );
};
