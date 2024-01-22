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
  BuyCredits: { resourceType: "WORKSPACE", role: "ADMIN" },

  // builder
  CreateCustomConnector: { resourceType: "WORKSPACE", role: "EDITOR" },
  UpdateCustomConnector: { resourceType: "WORKSPACE", role: "EDITOR" },

  // source
  CreateSource: { resourceType: "WORKSPACE", role: "EDITOR" },
  EditSource: { resourceType: "WORKSPACE", role: "EDITOR" },

  // destination
  CreateDestination: { resourceType: "WORKSPACE", role: "EDITOR" },
  EditDestination: { resourceType: "WORKSPACE", role: "EDITOR" },

  // connection
  CreateConnection: { resourceType: "WORKSPACE", role: "EDITOR" },
  SyncConnection: { resourceType: "WORKSPACE", role: "EDITOR" },
  EditConnection: { resourceType: "WORKSPACE", role: "EDITOR" },
} as const;
