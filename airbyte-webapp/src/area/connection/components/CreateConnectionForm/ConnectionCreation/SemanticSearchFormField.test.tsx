import { fireEvent, render, screen, waitFor } from "@testing-library/react";

import { TestWrapper } from "test-utils";

import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { ConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn";
import { useConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess";
import { useConnectionSemanticSearchEnablement, useUpdateConnectionSemanticSearchEnablement } from "core/api";
import { useIntent } from "core/utils/rbac";

import { SemanticSearchFormField } from "./SemanticSearchFormField";

jest.mock("area/connection/utils/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: jest.fn(),
}));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: jest.fn() }));
jest.mock("cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess", () => ({
  useConnectionSemanticSearchAccess: jest.fn(),
}));
jest.mock("core/api", () => ({
  useConnectionSemanticSearchEnablement: jest.fn(),
  useUpdateConnectionSemanticSearchEnablement: jest.fn(),
}));
jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useIntent: jest.fn(),
}));

const mockUseConnectionFormService = useConnectionFormService as jest.MockedFunction<typeof useConnectionFormService>;
const mockUseCurrentWorkspaceId = useCurrentWorkspaceId as jest.MockedFunction<typeof useCurrentWorkspaceId>;
const mockUseAccess = useConnectionSemanticSearchAccess as jest.MockedFunction<
  typeof useConnectionSemanticSearchAccess
>;
const mockUseEnablement = useConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useConnectionSemanticSearchEnablement
>;
const mockUseUpdateEnablement = useUpdateConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useUpdateConnectionSemanticSearchEnablement
>;
const mockUseIntent = useIntent as jest.MockedFunction<typeof useIntent>;

const SWITCH_NAME = "GitHub → Snowflake semantic search";

const mockSavedValue = (enabled: boolean) =>
  mockUseEnablement.mockReturnValue({
    data: { connectionId: "connection-1", enable_indexing: enabled },
    isLoading: false,
    isError: false,
  } as never);

const mockUpdate = (overrides: Record<string, unknown> = {}) => {
  const update = { mutateAsync: jest.fn().mockResolvedValue(undefined), isLoading: false, ...overrides };
  mockUseUpdateEnablement.mockReturnValue(update as never);
  return update;
};

// The intent and the access hook agree in the app: "noPermission" means the intent is false.
const mockAccess = (access: ConnectionSemanticSearchAccess) => {
  mockUseAccess.mockReturnValue(access);
  mockUseIntent.mockReturnValue(access !== "noPermission");
};

const renderField = (props: { isCreating?: boolean; disabled?: boolean } = {}) =>
  render(
    <TestWrapper>
      <SemanticSearchFormField
        isCreating={props.isCreating ?? false}
        sourceDefinitionId="source-definition"
        destinationDefinitionId="destination-definition"
        disabled={props.disabled}
      />
    </TestWrapper>
  );

describe("SemanticSearchFormField", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseConnectionFormService.mockReturnValue({
      connection: { connectionId: "connection-1", name: "GitHub → Snowflake" },
    } as never);
    mockUseCurrentWorkspaceId.mockReturnValue("workspace-1");
    mockAccess("editable");
    mockSavedValue(true);
    mockUpdate();
  });

  it("renders nothing on the create step", () => {
    renderField({ isCreating: true });

    expect(screen.queryByText("Semantic search")).not.toBeInTheDocument();
    expect(mockUseAccess).not.toHaveBeenCalled();
    expect(mockUseEnablement).not.toHaveBeenCalled();
  });

  it("renders nothing when the access is hidden", () => {
    mockAccess("hidden");

    renderField();

    expect(mockUseAccess).toHaveBeenCalledWith({
      sourceDefinitionId: "source-definition",
      destinationDefinitionId: "destination-definition",
    });
    expect(screen.queryByText("Semantic search")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(mockUseEnablement).not.toHaveBeenCalled();
  });

  it("shows the label and the description", () => {
    renderField();

    expect(screen.getByText("Semantic search")).toBeVisible();
    expect(
      screen.getByText(
        "Agents can use the Airbyte MCP to search and reason about your data with greater accuracy and efficiency."
      )
    ).toBeVisible();
  });

  it("locks the switch off and says an organization admin can turn it on when the organization setting is off", () => {
    mockAccess("orgOff");

    renderField();

    const control = screen.getByRole("checkbox");
    expect(control).toBeDisabled();
    expect(control).not.toBeChecked();
    expect(mockUseEnablement).not.toHaveBeenCalled();
    fireEvent.mouseOver(control);
    expect(
      screen.getByText(
        "Semantic search is off for this organization. An organization admin can turn it on in Context layer settings."
      )
    ).toBeVisible();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("shows the saved value in a disabled switch with a tooltip when the viewer cannot edit", () => {
    mockAccess("noPermission");

    renderField();

    const control = screen.getByRole("checkbox", { name: SWITCH_NAME });
    expect(control).toBeDisabled();
    expect(control).toBeChecked();
    expect(mockUseIntent).toHaveBeenCalledWith("EditConnectionSemanticSearch", { workspaceId: "workspace-1" });
    fireEvent.mouseOver(control);
    expect(screen.getByText("You need edit permission for this workspace to change semantic search.")).toBeVisible();
  });

  it("disables the switch for a deprecated connection", () => {
    renderField({ disabled: true });

    expect(screen.getByRole("checkbox", { name: SWITCH_NAME })).toBeDisabled();
  });

  it("saves at once when the viewer turns semantic search on", async () => {
    mockSavedValue(false);
    const update = mockUpdate();

    renderField();
    const control = screen.getByRole("checkbox", { name: SWITCH_NAME });
    expect(control).toBeEnabled();
    fireEvent.click(control);

    await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: true, expectedEnabled: false }));
    expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument();
  });

  it("asks for confirmation before it turns semantic search off", async () => {
    const update = mockUpdate();

    renderField();
    fireEvent.click(screen.getByRole("checkbox", { name: SWITCH_NAME }));

    expect(await screen.findByText("Turn off semantic search")).toBeVisible();
    expect(
      screen.getByText(
        "Turning off semantic search also deletes your semantic index. If you enable this again, you will need to backfill your data again, which will incur additional compute costs on your destination."
      )
    ).toBeVisible();
    expect(update.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument());
    expect(update.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("checkbox", { name: SWITCH_NAME }));
    fireEvent.click(await screen.findByRole("button", { name: "Delete semantic index" }));

    await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: false, expectedEnabled: true }));
  });
});
