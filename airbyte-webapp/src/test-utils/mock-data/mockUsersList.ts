import {
  OrganizationUserRead,
  OrganizationUserReadList,
  UserInvitationRead,
  WorkspaceUserAccessInfoRead,
  WorkspaceUserReadList,
} from "core/api/types/AirbyteClient";

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

export const mockOrganizationUsers: OrganizationUserRead[] = [
  {
    userId: "orgUser1",
    name: "Org User 1",
    email: "orguser1@test.com",
    permissionType: "organization_member",
    permissionId: "permission1",
    organizationId: "org-id",
  },
  {
    userId: "orgUser2",
    name: "Org User 2",
    email: "orguser2@test.com",
    permissionType: "organization_reader",
    permissionId: "permission2",
    organizationId: "org-id",
  },
  {
    userId: "orgUser3",
    name: "Org User 3",
    email: "orguser3@test.com",
    permissionType: "organization_admin",
    permissionId: "permission6",
    organizationId: "org-id",
  },
  {
    userId: "orgAndWorkspaceUser",
    name: "Org And Workspace User",
    email: "organdworkspaceuser@test.com",
    permissionType: "organization_member",
    permissionId: "permission3",
    organizationId: "org-id",
  },
];

export const mockWorkspaceAccessUsers: WorkspaceUserAccessInfoRead[] = [
  {
    userId: "workspaceUser1",
    userName: "Ws User 1",
    userEmail: "wsuser1@test.com",
    workspaceId: "ws-id",
    workspacePermission: { permissionType: "workspace_admin", permissionId: "permission4", userId: "workspaceUser1" },
  },
  {
    userId: "orgUser2",
    workspaceId: "ws-id",
    userName: "Org User 2",
    userEmail: "orguser2@test.com",
    organizationPermission: {
      permissionType: "organization_reader",
      permissionId: "permission2",
      organizationId: "org-id",
      userId: "orgUser2",
    },
  },
  {
    userId: "orgAndWorkspaceUser",
    userName: "Org And Workspace User",
    userEmail: "organdworkspaceuser@test.com",
    workspaceId: "ws-id",
    workspacePermission: {
      permissionType: "workspace_editor",
      permissionId: "permission5",
      userId: "orgAndWorkspaceUser",
    },
    organizationPermission: {
      userId: "orgAndWorkspaceUser",
      permissionId: "permission3",
      permissionType: "organization_member",
    },
  },
];

export const mockWorkspaceUserInvitations: UserInvitationRead[] = [
  {
    inviteCode: "code1",
    invitedEmail: "userInvite1@example.com",
    status: "pending",
    permissionType: "workspace_reader",
    createdAt: 1727469930,
    updatedAt: 1727469930,
    id: "invitation1",
    inviterUserId: "inviter1",
    scopeId: "scope1",
    scopeType: "workspace",
  },
];

export const mockOrganizationUserInvitations: UserInvitationRead[] = [
  {
    inviteCode: "code1",
    invitedEmail: "userInvite1@example.com",
    status: "pending",
    permissionType: "organization_reader",
    createdAt: 1727469930,
    updatedAt: 1727469930,
    id: "invitation1",
    inviterUserId: "inviter1",
    scopeId: "scope1",
    scopeType: "organization",
  },
];
