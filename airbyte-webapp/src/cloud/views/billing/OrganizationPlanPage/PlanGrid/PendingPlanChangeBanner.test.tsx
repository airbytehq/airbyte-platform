import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useUnschedulePlanChange } from "core/api";
import { useNotificationService } from "core/services/Notification";

import { PendingPlanChangeBanner } from "./PendingPlanChangeBanner";

jest.mock("core/api", () => ({
  useUnschedulePlanChange: jest.fn(),
}));

jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
}));

const mutateAsync = jest.fn();
const registerNotification = jest.fn();

beforeEach(() => {
  jest.clearAllMocks();
  mocked(useUnschedulePlanChange).mockReturnValue({
    mutateAsync,
    isLoading: false,
  } as unknown as ReturnType<typeof useUnschedulePlanChange>);
  mocked(useNotificationService).mockReturnValue({
    registerNotification,
  } as unknown as ReturnType<typeof useNotificationService>);
});

describe("PendingPlanChangeBanner", () => {
  it("renders nothing when there is no pending plan change", async () => {
    await render(<PendingPlanChangeBanner organizationId="org-1" pendingPlanChange={undefined} />);

    expect(screen.queryByTestId("pending-plan-change-banner")).not.toBeInTheDocument();
  });

  it("renders the pending plan change message and a single cancel button with no close control", async () => {
    await render(
      <PendingPlanChangeBanner
        organizationId="org-1"
        pendingPlanChange={{ effectiveDate: "2030-10-01T00:00:00Z", planName: "Plus" }}
      />
    );

    const banner = screen.getByTestId("pending-plan-change-banner");
    expect(banner).toHaveTextContent("Your plan will change to");
    expect(banner).toHaveTextContent("Plus");
    const cancelButton = within(banner).getByRole("button", { name: "Cancel plan change" });
    expect(cancelButton).toBeInTheDocument();
    expect(within(banner).getAllByRole("button")).toHaveLength(1);
  });

  it("unschedules the plan change and shows a success notification", async () => {
    mutateAsync.mockResolvedValue(undefined);

    await render(
      <PendingPlanChangeBanner
        organizationId="org-1"
        pendingPlanChange={{ effectiveDate: "2030-10-01T00:00:00Z", planName: "Plus" }}
      />
    );

    await userEvent.click(screen.getByRole("button", { name: "Cancel plan change" }));

    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(registerNotification).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "planChangeUnscheduled",
        type: "success",
      })
    );
  });

  it("shows an error notification when unscheduling fails", async () => {
    mutateAsync.mockRejectedValue(new Error("Something went wrong"));

    await render(
      <PendingPlanChangeBanner
        organizationId="org-1"
        pendingPlanChange={{ effectiveDate: "2030-10-01T00:00:00Z", planName: "Plus" }}
      />
    );

    await userEvent.click(screen.getByRole("button", { name: "Cancel plan change" }));

    expect(registerNotification).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "planChangeUnscheduleError",
        type: "error",
      })
    );
  });
});
