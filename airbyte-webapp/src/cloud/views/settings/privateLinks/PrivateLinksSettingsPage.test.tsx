import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { HttpError, useCreatePrivateLink, useDeletePrivateLink, useListPrivateLinks } from "core/api";
import { PrivateLinkRead, PrivateLinkStatus } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";

import { PrivateLinksSettingsPage } from "./PrivateLinksSettingsPage";

jest.mock("core/api", () => {
  class MockHttpError extends Error {
    request: unknown;
    status: number;
    response: unknown;
    constructor(request: unknown, status: number, response: unknown) {
      super(`HTTP ${status}`);
      this.request = request;
      this.status = status;
      this.response = response;
    }
  }
  return {
    HttpError: MockHttpError,
    useCreatePrivateLink: jest.fn(),
    useDeletePrivateLink: jest.fn(),
    useListPrivateLinks: jest.fn(),
  };
});

jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
}));

jest.mock("core/utils/datadog", () => ({
  trackError: jest.fn(),
}));

const mockUseCreatePrivateLink = mocked(useCreatePrivateLink);
const mockUseDeletePrivateLink = mocked(useDeletePrivateLink);
const mockUseListPrivateLinks = mocked(useListPrivateLinks);
const mockUseNotificationService = mocked(useNotificationService);
const mockTrackError = mocked(trackError);

const buildPrivateLink = (overrides: Partial<PrivateLinkRead> = {}): PrivateLinkRead => ({
  id: "pl-1",
  workspaceId: "ws-1",
  dataplaneGroupId: "dp-1",
  name: "test-link",
  status: PrivateLinkStatus.available,
  serviceRegion: "us-east-1",
  serviceName: "com.amazonaws.vpce.us-east-1.vpce-svc-abc123",
  endpointId: "vpce-123",
  dnsName: "vpce-123.us-east-1.vpce.amazonaws.com",
  createdAt: "2026-01-01T00:00:00Z",
  updatedAt: "2026-01-01T00:00:00Z",
  ...overrides,
});

const getSubmitButton = () =>
  screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];

const fillForm = async (name: string, serviceName: string) => {
  await userEvent.type(screen.getByLabelText("Name"), name);
  await userEvent.type(screen.getByLabelText(/Endpoint Service Name/i), serviceName);
  const submitButton = getSubmitButton();
  await waitFor(() => expect(submitButton).toBeEnabled());
  await userEvent.click(submitButton);
};

jest.setTimeout(20000);

describe("PrivateLinksSettingsPage", () => {
  const mockCreate = jest.fn();
  const mockDelete = jest.fn();
  const mockRegisterNotification = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCreatePrivateLink.mockReturnValue({
      mutateAsync: mockCreate,
    } as unknown as ReturnType<typeof useCreatePrivateLink>);
    mockUseDeletePrivateLink.mockReturnValue({
      mutateAsync: mockDelete,
    } as unknown as ReturnType<typeof useDeletePrivateLink>);
    mockUseNotificationService.mockReturnValue({
      registerNotification: mockRegisterNotification,
    } as unknown as ReturnType<typeof useNotificationService>);
  });

  it("renders empty state when no private links exist", async () => {
    mockUseListPrivateLinks.mockReturnValue({ privateLinks: [] } as unknown as ReturnType<typeof useListPrivateLinks>);

    await render(<PrivateLinksSettingsPage />);

    expect(screen.getByText(/No private links/i)).toBeInTheDocument();
  });

  it("renders the table with existing private links", async () => {
    mockUseListPrivateLinks.mockReturnValue({
      privateLinks: [buildPrivateLink({ name: "production-db" })],
    } as unknown as ReturnType<typeof useListPrivateLinks>);

    await render(<PrivateLinksSettingsPage />);

    expect(screen.getByText("production-db")).toBeInTheDocument();
    expect(screen.getByText("vpce-123.us-east-1.vpce.amazonaws.com")).toBeInTheDocument();
  });

  it("submits the create form with parsed region from service name", async () => {
    mockUseListPrivateLinks.mockReturnValue({ privateLinks: [] } as unknown as ReturnType<typeof useListPrivateLinks>);
    mockCreate.mockResolvedValue(buildPrivateLink());

    await render(<PrivateLinksSettingsPage />);

    await fillForm("my-link", "com.amazonaws.vpce.eu-west-1.vpce-svc-xyz789");

    await waitFor(() => {
      expect(mockCreate).toHaveBeenCalledWith({
        name: "my-link",
        serviceRegion: "eu-west-1",
        serviceName: "com.amazonaws.vpce.eu-west-1.vpce-svc-xyz789",
      });
    });
  });

  it("rejects duplicate names in the workspace", async () => {
    mockUseListPrivateLinks.mockReturnValue({
      privateLinks: [buildPrivateLink({ name: "existing-link" })],
    } as unknown as ReturnType<typeof useListPrivateLinks>);

    await render(<PrivateLinksSettingsPage />);

    await fillForm("existing-link", "com.amazonaws.vpce.us-east-1.vpce-svc-abc123");

    await waitFor(() => {
      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({ id: "privateLinks/create-failure", type: "error" })
      );
    });
    expect(mockCreate).not.toHaveBeenCalled();
    expect(mockTrackError).not.toHaveBeenCalled();
  });

  it("surfaces backend error message when create fails (e.g. limit reached)", async () => {
    mockUseListPrivateLinks.mockReturnValue({ privateLinks: [] } as unknown as ReturnType<typeof useListPrivateLinks>);
    const httpError = new HttpError({} as never, 400, {
      message: "You have reached your Private Link limit of 5.",
    });
    mockCreate.mockRejectedValue(httpError);

    await render(<PrivateLinksSettingsPage />);

    await fillForm("another-link", "com.amazonaws.vpce.us-east-1.vpce-svc-abc123");

    await waitFor(() => {
      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          text: "You have reached your Private Link limit of 5.",
          type: "error",
        })
      );
    });
    expect(mockTrackError).toHaveBeenCalledWith(httpError);
  });

  it("hides actions menu entirely for links in non-deletable, non-viewable status", async () => {
    mockUseListPrivateLinks.mockReturnValue({
      privateLinks: [buildPrivateLink({ status: PrivateLinkStatus.creating })],
    } as unknown as ReturnType<typeof useListPrivateLinks>);

    await render(<PrivateLinksSettingsPage />);

    // Creating status has no view and no delete action — the actions menu button should not be rendered
    expect(screen.queryByRole("button", { name: /Actions/i })).not.toBeInTheDocument();
  });

  it("opens delete confirmation modal and deletes the link", async () => {
    mockUseListPrivateLinks.mockReturnValue({
      privateLinks: [buildPrivateLink({ name: "to-delete" })],
    } as unknown as ReturnType<typeof useListPrivateLinks>);
    mockDelete.mockResolvedValue(undefined);

    await render(<PrivateLinksSettingsPage />);

    await userEvent.click(screen.getByRole("button", { name: /Actions/i }));
    // Click the "Delete" dropdown option
    await userEvent.click(await screen.findByRole("menuitem", { name: /Delete/i }));

    // Confirmation modal opens — submit it (dialog has its own submit button)
    const dialog = await screen.findByRole("dialog");
    const confirmButton = within(dialog)
      .getAllByRole("button")
      .find((b) => b.textContent?.trim() === "Delete");
    expect(confirmButton).toBeDefined();
    await userEvent.click(confirmButton!);

    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith("pl-1"));
  });

  it("opens view details modal showing private link metadata", async () => {
    mockUseListPrivateLinks.mockReturnValue({
      privateLinks: [
        buildPrivateLink({
          id: "pl-detail-id",
          name: "detailed-link",
          endpointId: "vpce-detail-123",
        }),
      ],
    } as unknown as ReturnType<typeof useListPrivateLinks>);

    await render(<PrivateLinksSettingsPage />);

    await userEvent.click(screen.getByRole("button", { name: /Actions/i }));
    await userEvent.click(await screen.findByText(/View details/i));

    await waitFor(() => expect(screen.getByText("pl-detail-id")).toBeInTheDocument());
    expect(screen.getByText("vpce-detail-123")).toBeInTheDocument();
  });
});
