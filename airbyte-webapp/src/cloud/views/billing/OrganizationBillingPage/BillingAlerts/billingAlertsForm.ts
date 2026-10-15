import { z } from "zod";

import {
  BillingNotificationSettingsRead,
  BillingNotificationSettingsUpdateRequestBody,
} from "core/api/types/AirbyteClient";

const MAX_EMAIL_LENGTH = 256;
const THRESHOLD_ERROR_MESSAGE_ID = "settings.organization.billing.alerts.threshold.invalid";
// A missing address turns the helper sentence red instead of adding a second message.
const EMAIL_REQUIRED_MESSAGE_ID = "settings.organization.billing.alerts.email.helper";

export interface BillingAlertsFormValues {
  email: string;
  syncAlertEnabled: boolean;
  syncCreditThreshold: string;
  invoiceAlertEnabled: boolean;
  invoiceAmountThreshold: string;
}

const emailSchema = z.string().email();

const isPositiveWholeNumber = (value: string) => {
  const trimmedValue = value.trim();
  return /^\d+$/.test(trimmedValue) && Number.isSafeInteger(Number(trimmedValue)) && Number(trimmedValue) > 0;
};

export const billingAlertsFormSchema = z
  .object({
    email: z
      .string()
      .trim()
      .min(1, EMAIL_REQUIRED_MESSAGE_ID)
      .max(MAX_EMAIL_LENGTH, "settings.organization.billing.alerts.email.tooLong")
      // An empty address is reported by the required check alone, not also as malformed.
      .refine((email) => email === "" || emailSchema.safeParse(email).success, "form.email.error"),
    syncAlertEnabled: z.boolean(),
    syncCreditThreshold: z.string(),
    invoiceAlertEnabled: z.boolean(),
    invoiceAmountThreshold: z.string(),
  })
  .superRefine((values, context) => {
    if (values.syncAlertEnabled && !isPositiveWholeNumber(values.syncCreditThreshold)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["syncCreditThreshold"],
        message: THRESHOLD_ERROR_MESSAGE_ID,
      });
    }
    if (values.invoiceAlertEnabled && !isPositiveWholeNumber(values.invoiceAmountThreshold)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["invoiceAmountThreshold"],
        message: THRESHOLD_ERROR_MESSAGE_ID,
      });
    }
  });

// The backend stores 0 for an alert that is off. The form shows that as an off toggle with an empty input.
const thresholdToInputValue = (threshold: number) => (threshold > 0 ? String(threshold) : "");

export const toFormValues = (settings: BillingNotificationSettingsRead): BillingAlertsFormValues => ({
  email: settings.recipientEmail ?? "",
  syncAlertEnabled: settings.syncCreditThreshold > 0,
  syncCreditThreshold: thresholdToInputValue(settings.syncCreditThreshold),
  invoiceAlertEnabled: settings.invoiceAmountThreshold > 0,
  invoiceAmountThreshold: thresholdToInputValue(settings.invoiceAmountThreshold),
});

export const toUpdateRequest = (
  values: BillingAlertsFormValues
): Omit<BillingNotificationSettingsUpdateRequestBody, "organizationId"> => {
  return {
    recipientEmail: values.email.trim(),
    syncCreditThreshold: values.syncAlertEnabled ? Number(values.syncCreditThreshold.trim()) : 0,
    invoiceAmountThreshold: values.invoiceAlertEnabled ? Number(values.invoiceAmountThreshold.trim()) : 0,
  };
};
