import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { useListAuditLogs, useListWorkspacesInOrganization } from "core/api";
import { AuditLogRead } from "core/api/types/AirbyteClient";

import { OrganizationAuditLogsPage } from "./OrganizationAuditLogsPage";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("core/api/hooks/auditLogs", () => ({
  ...jest.requireActual("core/api/hooks/auditLogs"),
  useListAuditLogs: jest.fn(),
}));

jest.mock("core/api/hooks/organizations", () => ({
  useListWorkspacesInOrganization: jest.fn(),
}));

jest.mock("core/services/analytics", () => ({
  ...jest.requireActual("core/services/analytics"),
  useTrackPage: jest.fn(),
}));

const ORGANIZATION_ID = "test-organization-id";
const WORKSPACE_ID = "workspace-1";
const TIMESTAMP = 1755800000000;

const buildEntry = (overrides: Partial<AuditLogRead> = {}): AuditLogRead => ({
  id: "entry-1",
  timestamp: TIMESTAMP,
  operation: "updateConnection",
  success: true,
  actor: { actorId: "user-1", email: "user-1@airbyte.io" },
  organizationId: ORGANIZATION_ID,
  workspaceId: WORKSPACE_ID,
  ...overrides,
});

const mockAuditLogsQuery = (data: { auditLogs: AuditLogRead[]; nextPageToken?: string } | undefined) => {
  (useListAuditLogs as jest.Mock).mockReturnValue({
    data,
    isLoading: false,
    isError: false,
  });
};

describe("OrganizationAuditLogsPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useListWorkspacesInOrganization as jest.Mock).mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: WORKSPACE_ID, name: "Workspace One" }] }] },
    });
    mockAuditLogsQuery({ auditLogs: [] });
  });

  it("renders audit log entries", async () => {
    mockAuditLogsQuery({
      auditLogs: [
        buildEntry(),
        buildEntry({
          id: "entry-2",
          operation: "updateSsoConfig",
          success: false,
          workspaceId: undefined,
          errorMessage: "boom",
          actor: { actorId: "user-2", email: "user-2@airbyte.io" },
        }),
      ],
    });

    await render(<OrganizationAuditLogsPage />);

    expect(screen.getByText("updateConnection")).toBeInTheDocument();
    expect(screen.getByText("updateSsoConfig")).toBeInTheDocument();
    expect(screen.getByText("user-1@airbyte.io")).toBeInTheDocument();
    expect(screen.getByText("user-2@airbyte.io")).toBeInTheDocument();
    // entry-1 resolves its workspace name from the org workspace list; entry-2 is org-level
    expect(screen.getByText("Workspace One")).toBeInTheDocument();
    expect(screen.getByText("boom")).toBeInTheDocument();
    expect(screen.getByText("Success")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
  });

  it("renders timestamps in UTC so they line up with the date filters", async () => {
    mockAuditLogsQuery({ auditLogs: [buildEntry()] });

    await render(<OrganizationAuditLogsPage />);

    // Computed here rather than hardcoded so the assertion holds whatever timezone the test
    // runner is in. DatePicker emits the picked wall-clock time as UTC, so the table must render
    // UTC too, otherwise the visible times are offset from the range the filters selected.
    const renderedAsUtc = `${new Intl.DateTimeFormat("en", { dateStyle: "short", timeZone: "UTC" }).format(
      TIMESTAMP
    )} ${new Intl.DateTimeFormat("en", { timeStyle: "medium", timeZone: "UTC" }).format(TIMESTAMP)}`;

    expect(screen.getByText(renderedAsUtc)).toBeInTheDocument();
  });

  it("does not crash while a date filter is partially typed", async () => {
    await render(<OrganizationAuditLogsPage />);

    // DatePicker forwards raw input on every keystroke, so the page sees half-typed values.
    // new Date("2026-0").toISOString() throws a RangeError, which used to take out the render.
    fireEvent.change(screen.getByPlaceholderText("Start time (UTC)"), { target: { value: "2026-0" } });

    expect(screen.getByText("No audit log entries match the selected filters.")).toBeInTheDocument();
  });

  it("omits a date filter that cannot be parsed", async () => {
    await render(<OrganizationAuditLogsPage />);

    fireEvent.change(screen.getByPlaceholderText("Start time (UTC)"), { target: { value: "not a date" } });

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(expect.objectContaining({ startTime: undefined }), undefined);
    });
  });

  it("sends a fully typed date filter as a UTC instant", async () => {
    await render(<OrganizationAuditLogsPage />);

    fireEvent.change(screen.getByPlaceholderText("Start time (UTC)"), {
      target: { value: "2026-08-14T10:00:00Z" },
    });

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(
        expect.objectContaining({ startTime: "2026-08-14T10:00:00.000Z" }),
        undefined
      );
    });
  });

  it("wires the actor filter to the query", async () => {
    mockAuditLogsQuery({
      auditLogs: [
        buildEntry({ id: "entry-1", actor: { actorId: "user-1", email: "user-1@airbyte.io" } }),
        buildEntry({ id: "entry-2", actor: { actorId: "user-2", email: "user-2@airbyte.io" } }),
      ],
    });
    await render(<OrganizationAuditLogsPage />);

    await userEvent.click(screen.getByRole("button", { name: "All actors" }));
    await userEvent.click(await screen.findByRole("option", { name: "user-2@airbyte.io" }));

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(
        expect.objectContaining({ actorId: "user-2@airbyte.io" }),
        undefined
      );
    });
  });

  it("wires the operation filter to the query", async () => {
    mockAuditLogsQuery({
      auditLogs: [
        buildEntry({ id: "entry-1", operation: "updateConnection" }),
        buildEntry({ id: "entry-2", operation: "enableScim" }),
      ],
    });
    await render(<OrganizationAuditLogsPage />);

    await userEvent.click(screen.getByRole("button", { name: "All operations" }));
    await userEvent.click(await screen.findByRole("option", { name: "enableScim" }));

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(
        expect.objectContaining({ operation: "enableScim" }),
        undefined
      );
    });
  });

  it("derives the actor and operation options from a query that ignores those filters", async () => {
    mockAuditLogsQuery({
      auditLogs: [
        buildEntry({ id: "entry-1", actor: { actorId: "user-1", email: "user-1@airbyte.io" } }),
        buildEntry({ id: "entry-2", actor: { actorId: "user-2", email: "user-2@airbyte.io" } }),
      ],
    });
    await render(<OrganizationAuditLogsPage />);

    await userEvent.click(screen.getByRole("button", { name: "All actors" }));
    await userEvent.click(await screen.findByRole("option", { name: "user-1@airbyte.io" }));

    // The option source query must not carry the actor filter, otherwise the dropdown would
    // collapse to the selected actor and there would be no way back to the others.
    expect(useListAuditLogs).toHaveBeenCalledWith(expect.not.objectContaining({ actorId: expect.anything() }));

    await userEvent.click(screen.getByRole("button", { name: "user-1@airbyte.io" }));
    expect(await screen.findByRole("option", { name: "user-2@airbyte.io" })).toBeInTheDocument();
  });

  it("wires the status filter to the query", async () => {
    await render(<OrganizationAuditLogsPage />);

    await userEvent.click(screen.getByRole("button", { name: "All statuses" }));
    await userEvent.click(await screen.findByRole("option", { name: "Failed" }));

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(expect.objectContaining({ success: false }), undefined);
    });
  });

  it("wires the workspace filter to the query", async () => {
    await render(<OrganizationAuditLogsPage />);

    await userEvent.click(screen.getByRole("button", { name: "All workspaces" }));
    await userEvent.click(await screen.findByRole("option", { name: "Workspace One" }));

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(
        expect.objectContaining({ workspaceId: WORKSPACE_ID }),
        undefined
      );
    });
  });

  it("paginates forward with the next page token and back", async () => {
    mockAuditLogsQuery({ auditLogs: [buildEntry()], nextPageToken: "token-2" });

    await render(<OrganizationAuditLogsPage />);

    const nextButton = screen.getByTestId("audit-logs-next-page");
    expect(screen.getByTestId("audit-logs-previous-page")).toBeDisabled();
    expect(nextButton).toBeEnabled();

    await userEvent.click(nextButton);

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(expect.anything(), "token-2");
    });
    expect(screen.getByTestId("audit-logs-previous-page")).toBeEnabled();

    await userEvent.click(screen.getByTestId("audit-logs-previous-page"));

    await waitFor(() => {
      expect(useListAuditLogs).toHaveBeenLastCalledWith(expect.anything(), undefined);
    });
  });

  it("renders an empty state when there are no entries", async () => {
    mockAuditLogsQuery({ auditLogs: [] });

    await render(<OrganizationAuditLogsPage />);

    expect(screen.getByText("No audit log entries match the selected filters.")).toBeInTheDocument();
  });

  it("renders an inline error when the query fails", async () => {
    (useListAuditLogs as jest.Mock).mockReturnValue({ data: undefined, isLoading: false, isError: true });

    await render(<OrganizationAuditLogsPage />);

    expect(screen.getByText("Failed to load audit logs.")).toBeInTheDocument();
  });
});
