import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import {
  useGetBillingNotificationSettings,
  useGetPaymentInformation,
  useUpdateBillingNotificationSettings,
} from "core/api";
import { BillingNotificationSettingsRead } from "core/api/types/AirbyteClient";

import { BillingAlerts } from "./BillingAlerts";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("core/api", () => ({
  useGetBillingNotificationSettings: jest.fn(),
  useUpdateBillingNotificationSettings: jest.fn(),
  useGetPaymentInformation: jest.fn(),
}));

const unconfiguredSettings: BillingNotificationSettingsRead = {
  organizationId: "test-organization-id",
  configured: false,
  syncCreditThreshold: 0,
  invoiceAmountThreshold: 0,
};

const configuredSettings: BillingNotificationSettingsRead = {
  organizationId: "test-organization-id",
  configured: true,
  recipientEmail: "alerts@example.com",
  syncCreditThreshold: 100,
  invoiceAmountThreshold: 2000,
};

const mockSettingsQuery = (query: { data?: BillingNotificationSettingsRead; isLoading?: boolean; isError?: boolean }) =>
  mocked(useGetBillingNotificationSettings).mockReturnValue({
    data: query.data,
    isLoading: query.isLoading ?? false,
    isError: query.isError ?? false,
  } as unknown as ReturnType<typeof useGetBillingNotificationSettings>);

const mockBillingEmail = (email?: string) =>
  mocked(useGetPaymentInformation).mockReturnValue({
    data: email ? { customer: { email } } : undefined,
  } as unknown as ReturnType<typeof useGetPaymentInformation>);

const mockUpdate = (mutateAsync: jest.Mock) =>
  mocked(useUpdateBillingNotificationSettings).mockReturnValue({
    mutateAsync,
  } as unknown as ReturnType<typeof useUpdateBillingNotificationSettings>);

const syncSwitch = () => screen.getByRole("checkbox", { name: "Email me when a single sync uses more than" });
const invoiceSwitch = () => screen.getByRole("checkbox", { name: "Email me when my upcoming invoice is more than" });
const syncInput = () => screen.getByRole("textbox", { name: "Credits used by a single sync" });
const invoiceInput = () => screen.getByRole("textbox", { name: "Upcoming invoice amount in U.S. dollars" });
const emailInput = () => screen.getByRole("textbox", { name: "Email" });
const saveButton = () => screen.getByRole("button", { name: "Save changes" });
const cancelButton = () => screen.getByRole("button", { name: "Cancel" });

describe("BillingAlerts", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockBillingEmail("invoices@example.com");
    mockUpdate(jest.fn());
  });

  it("shows an organization without saved settings with both alerts off and empty inputs", async () => {
    mockSettingsQuery({ data: unconfiguredSettings });

    await render(<BillingAlerts />);

    expect(syncSwitch()).not.toBeChecked();
    expect(invoiceSwitch()).not.toBeChecked();
    expect(syncInput()).toBeDisabled();
    expect(syncInput()).toHaveValue("");
    expect(syncInput()).toHaveAttribute("placeholder", "100");
    expect(invoiceInput()).toBeDisabled();
    expect(invoiceInput()).toHaveValue("");
    expect(invoiceInput()).toHaveAttribute("placeholder", "2000");
    expect(emailInput()).toHaveValue("");
    expect(saveButton()).toBeDisabled();
    expect(cancelButton()).toBeEnabled();
  });

  it("shows the billing email as the email placeholder", async () => {
    mockSettingsQuery({ data: unconfiguredSettings });

    await render(<BillingAlerts />);

    expect(emailInput()).toHaveAttribute("placeholder", "invoices@example.com");
  });

  it("falls back to an example email placeholder without payment information", async () => {
    mockBillingEmail(undefined);
    mockSettingsQuery({ data: unconfiguredSettings });

    await render(<BillingAlerts />);

    expect(emailInput()).toHaveAttribute("placeholder", "you@example.com");
  });

  it("shows saved settings with enabled alerts", async () => {
    mockSettingsQuery({ data: configuredSettings });

    await render(<BillingAlerts />);

    expect(emailInput()).toHaveValue("alerts@example.com");
    expect(syncSwitch()).toBeChecked();
    expect(syncInput()).toBeEnabled();
    expect(syncInput()).toHaveValue("100");
    expect(invoiceSwitch()).toBeChecked();
    expect(invoiceInput()).toHaveValue("2000");
    expect(saveButton()).toBeDisabled();
  });

  it("shows a loading state and no form while the settings load", async () => {
    mockSettingsQuery({ isLoading: true });

    await render(<BillingAlerts />);

    expect(screen.getByText("Billing alerts")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save changes" })).not.toBeInTheDocument();
  });

  it("shows an error and no form when the settings fail to load", async () => {
    mockSettingsQuery({ isError: true });

    await render(<BillingAlerts />);

    expect(screen.getByText("Error loading billing alerts")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save changes" })).not.toBeInTheDocument();
  });

  it("keeps the loaded form and shows no error when a refresh fails", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const wrapper = await render(<BillingAlerts />);

    mockSettingsQuery({ data: configuredSettings, isError: true });
    // The shared render helper wraps the tree in a div; keep it so the form stays mounted.
    wrapper.rerender(
      <div>
        <BillingAlerts />
      </div>
    );

    expect(syncInput()).toHaveValue("100");
    expect(screen.queryByText("Error loading billing alerts")).not.toBeInTheDocument();
  });

  it("keeps save disabled while an enabled alert has no value", async () => {
    mockSettingsQuery({ data: unconfiguredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.click(syncSwitch());

    expect(syncInput()).toBeEnabled();
    expect(saveButton()).toBeDisabled();
  });

  it.each(["0", "abc", "12.5"])("marks %p as invalid and keeps save disabled", async (value) => {
    mockSettingsQuery({ data: unconfiguredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.click(syncSwitch());
    await user.type(syncInput(), value);

    await waitFor(() => expect(syncInput()).toHaveAttribute("aria-invalid", "true"));
    const message = screen.getByText("Enter a whole number greater than 0");
    expect(syncInput()).toHaveAttribute("aria-describedby", message.id);
    expect(saveButton()).toBeDisabled();
  });

  it("shows refreshed settings in an untouched form", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const wrapper = await render(<BillingAlerts />);
    expect(syncInput()).toHaveValue("100");

    mockSettingsQuery({ data: { ...configuredSettings, syncCreditThreshold: 500 } });
    // The shared render helper wraps the tree in a div; keep it so the form stays mounted.
    wrapper.rerender(
      <div>
        <BillingAlerts />
      </div>
    );

    await waitFor(() => expect(syncInput()).toHaveValue("500"));
    expect(saveButton()).toBeDisabled();
  });

  it("keeps the user's edits when settings refresh behind a dirty form", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    const wrapper = await render(<BillingAlerts />);
    await user.clear(syncInput());
    await user.type(syncInput(), "250");

    mockSettingsQuery({ data: { ...configuredSettings, syncCreditThreshold: 500 } });
    // The shared render helper wraps the tree in a div; keep it so the form stays mounted.
    wrapper.rerender(
      <div>
        <BillingAlerts />
      </div>
    );

    expect(syncInput()).toHaveValue("250");
  });

  it("shows the full email helper text", async () => {
    mockSettingsQuery({ data: unconfiguredSettings });

    await render(<BillingAlerts />);

    expect(screen.getByText("This is the email where you’ll receive notifications.")).toBeInTheDocument();
  });

  it("saves the entered thresholds and sends 0 for the alert that is off", async () => {
    const mutateAsync = jest.fn().mockResolvedValue({
      ...unconfiguredSettings,
      configured: true,
      syncCreditThreshold: 150,
    });
    mockUpdate(mutateAsync);
    mockSettingsQuery({ data: unconfiguredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.type(emailInput(), "new@example.com");
    await user.click(syncSwitch());
    await user.type(syncInput(), "150");
    await waitFor(() => expect(saveButton()).toBeEnabled());
    await user.click(saveButton());

    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        recipientEmail: "new@example.com",
        syncCreditThreshold: 150,
        invoiceAmountThreshold: 0,
      })
    );
    expect(await screen.findByText("Billing alert settings saved.")).toBeInTheDocument();
    await waitFor(() => expect(saveButton()).toBeDisabled());
    expect(syncInput()).toHaveValue("150");
  });

  it("sends the typed email address", async () => {
    const mutateAsync = jest.fn().mockResolvedValue(configuredSettings);
    mockUpdate(mutateAsync);
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.clear(emailInput());
    await user.type(emailInput(), "new@example.com");
    await waitFor(() => expect(saveButton()).toBeEnabled());
    await user.click(saveButton());

    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        recipientEmail: "new@example.com",
        syncCreditThreshold: 100,
        invoiceAmountThreshold: 2000,
      })
    );
  });

  it("clears the input and saves 0 when an alert is switched off", async () => {
    const mutateAsync = jest.fn().mockResolvedValue({ ...configuredSettings, syncCreditThreshold: 0 });
    mockUpdate(mutateAsync);
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.click(syncSwitch());

    expect(syncInput()).toBeDisabled();
    expect(syncInput()).toHaveValue("");
    await waitFor(() => expect(saveButton()).toBeEnabled());
    await user.click(saveButton());

    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        recipientEmail: "alerts@example.com",
        syncCreditThreshold: 0,
        invoiceAmountThreshold: 2000,
      })
    );
  });

  it("flags a malformed email and keeps save disabled", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.clear(emailInput());
    await user.type(emailInput(), "not-an-email");

    expect(await screen.findByText("Must be a valid email")).toBeInTheDocument();
    expect(saveButton()).toBeDisabled();
  });

  it("requires an email and shows the helper sentence as the error", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.clear(emailInput());

    await waitFor(() => expect(emailInput()).toHaveAttribute("aria-invalid", "true"));
    expect(screen.getByText("This is the email where you’ll receive notifications.")).toBeInTheDocument();
    expect(saveButton()).toBeDisabled();
  });

  it("keeps the input and shows the error when the save fails", async () => {
    const mutateAsync = jest.fn().mockRejectedValue(new Error("Could not save"));
    mockUpdate(mutateAsync);
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.clear(syncInput());
    await user.type(syncInput(), "250");
    await waitFor(() => expect(saveButton()).toBeEnabled());
    await user.click(saveButton());

    expect(await screen.findByText("Could not save")).toBeInTheDocument();
    expect(syncInput()).toHaveValue("250");
    expect(saveButton()).toBeEnabled();
  });

  it("resets the form to the saved values on cancel", async () => {
    mockSettingsQuery({ data: configuredSettings });
    const user = userEvent.setup();
    await render(<BillingAlerts />);

    await user.clear(syncInput());
    await user.type(syncInput(), "250");
    await user.click(invoiceSwitch());
    await user.click(cancelButton());

    await waitFor(() => expect(syncInput()).toHaveValue("100"));
    expect(invoiceSwitch()).toBeChecked();
    expect(invoiceInput()).toHaveValue("2000");
    expect(saveButton()).toBeDisabled();
  });
});
