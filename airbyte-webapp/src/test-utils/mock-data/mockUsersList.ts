import { OrganizationUserReadList, WorkspaceUserReadList } from "core/api/types/AirbyteClient";

export const mockWorkspaceUserReadList: WorkspaceUserReadList = {
  users: [
    {
      email: "user0@test.com",
      name: "User Zero",
      permissionId: "000",
      permissionType: "workspace_admin",
      userId: "000",
      workspaceId: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
    },
    {
      email: "user1@test.com",
      name: "User One",
      permissionId: "001",
      permissionType: "workspace_reader",
      userId: "001",
      workspaceId: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
    },
  ],
};

export const mockOrganizationUserReadList: OrganizationUserReadList = {
  users: [
    {
      email: "user0@test.com",
      name: "User Zero",
      organizationId: "AAAA",
      permissionId: "002",
      permissionType: "organization_member",
      userId: "000",
    },
    {
      email: "user2@test.com",
      name: "User Two",
      organizationId: "AAAA",
      permissionId: "003",
      permissionType: "organization_member",
      userId: "002",
    },
    {
      email: "user3@test.com",
      name: "User Three",
      organizationId: "AAAA",
      permissionId: "004",
      permissionType: "organization_admin",
      userId: "003",
    },
  ],
};
