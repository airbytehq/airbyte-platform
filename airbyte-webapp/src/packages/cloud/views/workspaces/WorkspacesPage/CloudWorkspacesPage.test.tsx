import { mocked, render } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useListPermissions } from "core/api";
import { useListCloudWorkspacesInfinite } from "core/api/cloud";
import { OrganizationRead } from "core/api/types/AirbyteClient";

import { CloudWorkspacesPageInner } from "./CloudWorkspacesPage";

jest.mock("core/services/auth", () => ({
  useAuthService: () => ({}),
  useCurrentUser: () => ({
    userId: "123",
  }),
}));

jest.mock("core/api/cloud", () => ({
  useListCloudWorkspacesInfinite: jest.fn().mockReturnValue({
    data: undefined,
    isFetching: false,
  }),
  useCreateCloudWorkspace: () => ({
    mutateAsync: jest.fn(),
  }),
}));

jest.mock("core/api", () => ({
  useListPermissions: jest.fn().mockResolvedValue({
    permissions: [],
  }),
  useListOrganizationsById: (ids: string[]): OrganizationRead[] =>
    ids.map((id) => ({
      email: `${id}@example.com`,
      organizationId: id,
      organizationName: `Org ${id}`,
    })),
}));

describe("CloudWorkspacesPage", () => {
  describe("No Organization permission screen", () => {
    it("should show if you're member of only one organization and fetched no workspaces", async () => {
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          { permissionType: "organization_member", userId: "123", permissionId: "123", organizationId: "321" },
        ],
      });
      const wrapper = await render(<CloudWorkspacesPageInner />);
      expect(wrapper.queryByTestId("noWorkspacePermissionsBanner")).toBeInTheDocument();
      expect(wrapper.getByTestId("noWorkspacePermissionsBanner")).toHaveTextContent("321@example.com");
    });
    it("should show if you're member of multiple organizations and fetched no workspaces", async () => {
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          { permissionType: "organization_member", userId: "123", permissionId: "123", organizationId: "321" },
          { permissionType: "organization_member", userId: "123", permissionId: "123", organizationId: "456" },
        ],
      });
      const wrapper = await render(<CloudWorkspacesPageInner />);
      expect(wrapper.queryByTestId("noWorkspacePermissionsBanner")).toBeInTheDocument();
      expect(wrapper.getByTestId("noWorkspacePermissionsBanner")).toHaveTextContent("321@example.com");
    });
    it("organization member permissions do not supersede instance admin permissions in the check", async () => {
      mocked(useListCloudWorkspacesInfinite).mockReturnValue({
        isFetching: false,
        data: {
          pageParams: [],
          pages: [{ data: { workspaces: [mockWorkspace] }, pageParam: 0 }],
        },
      } as any); // eslint-disable-line @typescript-eslint/no-explicit-any
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          { permissionType: "organization_member", userId: "123", permissionId: "1", organizationId: "321" },
          { permissionType: "instance_admin", userId: "123", permissionId: "2" },
        ],
      });
      const wrapper = await render(<CloudWorkspacesPageInner />);
      expect(wrapper.queryByTestId("noWorkspacePermissionsBanner")).not.toBeInTheDocument();
    });
    it("should not show if you see any workspaces (e.g. as an instance admin)", async () => {
      mocked(useListCloudWorkspacesInfinite).mockReturnValue({
        isFetching: false,
        data: {
          pageParams: [],
          pages: [{ data: { workspaces: [mockWorkspace] }, pageParam: 0 }],
        },
      } as any); // eslint-disable-line @typescript-eslint/no-explicit-any
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          { permissionType: "organization_member", userId: "123", permissionId: "123", organizationId: "321" },
        ],
      });
      const wrapper = await render(<CloudWorkspacesPageInner />);
      expect(wrapper.queryByTestId("noWorkspacePermissionsBanner")).not.toBeInTheDocument();
    });

    it("should not show in case users can create workspaces", async () => {
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          { permissionType: "organization_member", userId: "123", permissionId: "1", organizationId: "321" },
          { permissionType: "organization_editor", userId: "123", permissionId: "2", organizationId: "456" },
        ],
      });
      const wrapper = await render(<CloudWorkspacesPageInner />);
      expect(wrapper.queryByTestId("noWorkspacePermissionsBanner")).not.toBeInTheDocument();
    });
  });
});
