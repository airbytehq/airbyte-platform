import { BillingNotificationSettingsRead } from "core/api/types/AirbyteClient";

import { BillingAlertsFormValues, billingAlertsFormSchema, toFormValues, toUpdateRequest } from "./billingAlertsForm";

const settings = (overrides: Partial<BillingNotificationSettingsRead> = {}): BillingNotificationSettingsRead => ({
  organizationId: "org-123",
  configured: true,
  syncCreditThreshold: 0,
  invoiceAmountThreshold: 0,
  ...overrides,
});

const formValues = (overrides: Partial<BillingAlertsFormValues> = {}): BillingAlertsFormValues => ({
  email: "",
  syncAlertEnabled: false,
  syncCreditThreshold: "",
  invoiceAlertEnabled: false,
  invoiceAmountThreshold: "",
  ...overrides,
});

describe("toFormValues", () => {
  it("shows an unconfigured organization with both alerts off and empty inputs", () => {
    expect(toFormValues(settings({ configured: false }))).toEqual(formValues());
  });

  it("shows a stored 0 as an off toggle with an empty input", () => {
    expect(toFormValues(settings({ syncCreditThreshold: 0, invoiceAmountThreshold: 500 }))).toEqual(
      formValues({ invoiceAlertEnabled: true, invoiceAmountThreshold: "500" })
    );
  });

  it("shows positive thresholds as on toggles with their values", () => {
    expect(
      toFormValues(settings({ recipientEmail: "a@b.co", syncCreditThreshold: 100, invoiceAmountThreshold: 2000 }))
    ).toEqual(
      formValues({
        email: "a@b.co",
        syncAlertEnabled: true,
        syncCreditThreshold: "100",
        invoiceAlertEnabled: true,
        invoiceAmountThreshold: "2000",
      })
    );
  });

  it("treats a null recipient email as empty", () => {
    expect(toFormValues(settings({ recipientEmail: null })).email).toBe("");
  });
});

describe("toUpdateRequest", () => {
  it("sends 0 for an alert that is off, whatever the input holds", () => {
    expect(toUpdateRequest(formValues({ syncCreditThreshold: "150", invoiceAmountThreshold: "900" }))).toEqual({
      recipientEmail: "",
      syncCreditThreshold: 0,
      invoiceAmountThreshold: 0,
    });
  });

  it("sends the numbers for alerts that are on", () => {
    expect(
      toUpdateRequest(
        formValues({
          syncAlertEnabled: true,
          syncCreditThreshold: " 150 ",
          invoiceAlertEnabled: true,
          invoiceAmountThreshold: "2000",
        })
      )
    ).toEqual({ recipientEmail: "", syncCreditThreshold: 150, invoiceAmountThreshold: 2000 });
  });

  it("trims the email", () => {
    expect(toUpdateRequest(formValues({ email: " a@b.co " })).recipientEmail).toBe("a@b.co");
  });
});

describe("billingAlertsFormSchema", () => {
  const issuePaths = (values: BillingAlertsFormValues) => {
    const result = billingAlertsFormSchema.safeParse(values);
    return result.success ? [] : result.error.issues.map((issue) => issue.path.join("."));
  };

  it("accepts the all-off form with an email", () => {
    expect(issuePaths(formValues({ email: "a@b.co" }))).toEqual([]);
  });

  it("requires an email and reports it with the helper message", () => {
    const result = billingAlertsFormSchema.safeParse(formValues({ email: "  " }));

    expect(result.success ? [] : result.error.issues.map((issue) => [issue.path.join("."), issue.message])).toEqual([
      ["email", "settings.organization.billing.alerts.email.helper"],
    ]);
  });

  it("accepts whole numbers above zero for enabled alerts", () => {
    expect(
      issuePaths(
        formValues({
          email: "a@b.co",
          syncAlertEnabled: true,
          syncCreditThreshold: "1",
          invoiceAlertEnabled: true,
          invoiceAmountThreshold: "2500",
        })
      )
    ).toEqual([]);
  });

  it.each(["", "0", "00", "abc", "12.5", "-3", "1e3", "9007199254740993"])(
    "rejects %p as the threshold of an enabled alert",
    (value) => {
      expect(issuePaths(formValues({ email: "a@b.co", syncAlertEnabled: true, syncCreditThreshold: value }))).toEqual([
        "syncCreditThreshold",
      ]);
      expect(
        issuePaths(formValues({ email: "a@b.co", invoiceAlertEnabled: true, invoiceAmountThreshold: value }))
      ).toEqual(["invoiceAmountThreshold"]);
    }
  );

  it("ignores the input of an alert that is off", () => {
    expect(
      issuePaths(formValues({ email: "a@b.co", syncCreditThreshold: "abc", invoiceAmountThreshold: "0" }))
    ).toEqual([]);
  });

  it("names the threshold message for a value that is not a whole number above zero", () => {
    const result = billingAlertsFormSchema.safeParse(
      formValues({ email: "a@b.co", syncAlertEnabled: true, syncCreditThreshold: "12.5" })
    );

    expect(result.success ? [] : result.error.issues.map((issue) => issue.message)).toEqual([
      "settings.organization.billing.alerts.threshold.invalid",
    ]);
  });

  it("rejects a malformed email", () => {
    expect(issuePaths(formValues({ email: "not-an-email" }))).toEqual(["email"]);
  });

  it("rejects an email longer than 256 characters with its own message", () => {
    const email = `${"a".repeat(250)}@example.com`;
    const result = billingAlertsFormSchema.safeParse(formValues({ email }));

    expect(result.success).toBe(false);
    expect(result.success ? [] : result.error.issues.map((issue) => issue.message)).toContain(
      "settings.organization.billing.alerts.email.tooLong"
    );
  });
});
