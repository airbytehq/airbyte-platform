import { waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { FeatureItem } from "core/services/features";
import messages from "locales/en.json";

import { NotificationSettingsForm } from "./NotificationSettingsForm";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({
    ...mockWorkspace,
    notificationSettings: { ...mockWorkspace.notificationSettings, sendOnConnectionUpdate: { notificationType: [] } },
  }),
  useTryNotificationWebhook: () => jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: () => true,
  useGeneratedIntent: () => true,
  Intent: {
    UpdateWorkspace: "UpdateWorkspace",
    CreateOrEditConnection: "CreateOrEditConnection",
  },
}));

const mockUpdateNotificationSettings = jest.fn();
jest.mock("hooks/services/useWorkspace", () => ({
  useUpdateNotificationSettings: () => mockUpdateNotificationSettings,
}));

describe(`${NotificationSettingsForm.name}`, () => {
  it("should render", async () => {
    const { getByTestId } = await render(<NotificationSettingsForm />);
    await waitFor(() => expect(getByTestId("notification-settings-form")).toBeInTheDocument());
  });

  it("should display not display email toggles if the feature is disabled", async () => {
    const { queryByTestId } = await render(<NotificationSettingsForm />, undefined, []);
    await waitFor(() => expect(queryByTestId("sendOnFailure.email")).toEqual(null));
  });

  it("should display display email toggles if the feature is enabled", async () => {
    const { getByTestId } = await render(<NotificationSettingsForm />, undefined, [FeatureItem.EmailNotifications]);
    await waitFor(() => expect(getByTestId("sendOnFailure.email")).toBeDefined());
  });

  it("calls updateNotificationSettings with the correct values", async () => {
    const { getByText, getByTestId } = await render(<NotificationSettingsForm />, undefined, [
      FeatureItem.EmailNotifications,
    ]);
    const sendOnSuccessToggle = getByTestId("sendOnConnectionUpdate.email");
    await userEvent.click(sendOnSuccessToggle);
    const submitButton = getByText(messages["form.saveChanges"]);
    await userEvent.click(submitButton);
    await waitFor(() => expect(mockUpdateNotificationSettings).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(mockUpdateNotificationSettings).toHaveBeenCalledWith({
        ...mockWorkspace.notificationSettings,
        sendOnConnectionUpdate: { notificationType: ["customerio"] },
      })
    );
  });
});
