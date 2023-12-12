export const intentToRbacQuery = {
  // instance

  // organization
  ListOrganizationMembers: { resourceType: "ORGANIZATION", role: "MEMBER" },
  UpdateConnectorVersions: { resourceType: "ORGANIZATION", role: "ADMIN" }, // this setting is currently instance-wide, but we've decided to scope to organization permissions for now
  UpdateOrganization: { resourceType: "ORGANIZATION", role: "ADMIN" },
  UpdateOrganizationPermissions: { resourceType: "ORGANIZATION", role: "ADMIN" },
  ViewOrganizationSettings: { resourceType: "ORGANIZATION", role: "READER" },

  // workspace
  DeleteWorkspace: { resourceType: "WORKSPACE", role: "ADMIN" },
  UpdateWorkspace: { resourceType: "WORKSPACE", role: "ADMIN" },
  UpdateWorkspacePermissions: { resourceType: "WORKSPACE", role: "ADMIN" },
  ViewWorkspaceSettings: { resourceType: "WORKSPACE", role: "READER" },
} as const;
