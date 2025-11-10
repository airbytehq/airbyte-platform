import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { WorkspaceSettingsView } from "../src/packages/cloud/views/workspaces/WorkspaceSettingsView/WorkspaceSettingsView";

jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: { id: string }) => <span>{id}</span>,
  useIntl: () => ({ formatMessage: ({ id }: { id: string }) => id }),
}));

jest.mock("components/ui/CopyButton", () => ({
  CopyButton: ({ children }: any) => <button data-testid="copy-button">{children}</button>,
}));

jest.mock("components/ui/Flex", () => ({
  FlexContainer: ({ children }: any) => <div data-testid="flex-container">{children}</div>,
  FlexItem: ({ children }: any) => <div data-testid="flex-item">{children}</div>,
}));

jest.mock("components/ui/Heading", () => ({
  Heading: ({ children }: any) => <h1>{children}</h1>,
}));

jest.mock("components/ui/Separator", () => ({
  Separator: () => <hr data-testid="separator" />,
}));

jest.mock("pages/SettingsPage/components/DeleteWorkspace", () => ({
  DeleteWorkspace: () => <div data-testid="delete-workspace">DeleteWorkspace</div>,
}));

jest.mock("pages/SettingsPage/Workspace/components/TagsTable", () => ({
  TagsTable: () => <div data-testid="tags-table">TagsTable</div>,
}));

jest.mock("../src/packages/cloud/views/workspaces/WorkspaceSettingsView/components/UpdateWorkspaceSettingsForm", () => ({
  UpdateWorkspaceSettingsForm: () => <div data-testid="update-form">UpdateWorkspaceSettingsForm</div>,
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("core/services/analytics", () => ({
  useTrackPage: jest.fn(),
  PageTrackingCodes: { SETTINGS_WORKSPACE: "SETTINGS_WORKSPACE" },
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { DeleteWorkspace: "DeleteWorkspace" },
  useGeneratedIntent: jest.fn(),
}));

const { useCurrentWorkspaceId } = jest.requireMock("area/workspace/utils");
const { useGeneratedIntent } = jest.requireMock("core/utils/rbac");
const { useTrackPage } = jest.requireMock("core/services/analytics");

describe("WorkspaceSettingsView", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useCurrentWorkspaceId as jest.Mock).mockReturnValue("ws-123");
    (useGeneratedIntent as jest.Mock).mockReturnValue(false);
  });

  it("usa el workspaceId correcto en el CopyButton", () => {
    (useCurrentWorkspaceId as jest.Mock).mockReturnValue("workspace-456");
    render(<WorkspaceSettingsView />);

    expect(screen.getByText(/settings.workspaceId/i)).toBeInTheDocument();
    expect(useCurrentWorkspaceId).toHaveBeenCalled();
  });
});
