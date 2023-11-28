export const intentToRbacQuery = {
  // instance

  // organization
  ListOrganizationMembers: { resourceType: "ORGANIZATION", role: "MEMBER" },
  UpdateOrganization: { resourceType: "ORGANIZATION", role: "ADMIN" },
  UpdateOrganizationPermissions: { resourceType: "ORGANIZATION", role: "ADMIN" },
  ViewOrganizationSettings: { resourceType: "ORGANIZATION", role: "READER" },

  // workspace
  DeleteWorkspace: { resourceType: "WORKSPACE", role: "ADMIN" },
  ListWorkspaceMembers: { resourceType: "WORKSPACE", role: "READER" },
  UpdateWorkspace: { resourceType: "WORKSPACE", role: "ADMIN" },
  UpdateWorkspacePermissions: { resourceType: "WORKSPACE", role: "ADMIN" },
  ViewWorkspaceSettings: { resourceType: "WORKSPACE", role: "READER" },
} as const;
