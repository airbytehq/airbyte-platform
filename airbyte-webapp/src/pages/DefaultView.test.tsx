import { mocked, render } from "test-utils";

import { useCurrentOrganizationId, useLastVisitedOrganizationId } from "area/organization/utils";
import { useListWorkspacesInfinite } from "core/api";

import { DefaultView } from "./DefaultView";

const mockNavigate = jest.fn();

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  Navigate: ({ to }: { to: string }) => {
    mockNavigate(to);
    return null;
  },
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useLastVisitedOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useListWorkspacesInfinite: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useCurrentUser: () => ({ metadata: {} }),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: () => false,
}));

jest.mock("core/utils/useRedirectFromChatConnectorBuilder", () => ({
  useRedirectFromChatConnectorBuilder: () => undefined,
}));

const mockWorkspaces = (workspaceIds: string[]) =>
  mocked(useListWorkspacesInfinite).mockReturnValue({
    data: { pages: [{ data: { workspaces: workspaceIds.map((workspaceId) => ({ workspaceId })) } }] },
  } as unknown as ReturnType<typeof useListWorkspacesInfinite>);

describe("DefaultView", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mocked(useLastVisitedOrganizationId).mockReturnValue(undefined);
  });

  it("shows a no-organization message instead of redirecting when the user has no workspace and no organization", async () => {
    mockWorkspaces([]);
    mocked(useCurrentOrganizationId).mockReturnValue(undefined as unknown as string);

    const wrapper = await render(<DefaultView />);

    expect(wrapper.getByText("You don't have access to any organization")).toBeInTheDocument();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  it("redirects to the only workspace", async () => {
    mockWorkspaces(["workspace-1"]);
    mocked(useCurrentOrganizationId).mockReturnValue("org-1");

    await render(<DefaultView />);

    expect(mockNavigate).toHaveBeenCalledWith("/workspaces/workspace-1");
  });

  it("redirects to the organization when it has no workspaces the user can see", async () => {
    mockWorkspaces([]);
    mocked(useCurrentOrganizationId).mockReturnValue("org-1");

    await render(<DefaultView />);

    expect(mockNavigate).toHaveBeenCalledWith("/organization/org-1/workspaces");
  });
});
