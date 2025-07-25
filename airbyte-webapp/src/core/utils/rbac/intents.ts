export const intentToRbacQuery = {
  // instance

  // organization
  ListOrganizationMembers: { resourceType: "ORGANIZATION", role: "MEMBER" },
  UpdateConnectorVersions: { resourceType: "ORGANIZATION", role: "ADMIN" }, // this setting is currently instance-wide, but we've decided to scope to organization permissions for now
  UpdateOrganization: { resourceType: "ORGANIZATION", role: "ADMIN" },
  UpdateOrganizationPermissions: { resourceType: "ORGANIZATION", role: "ADMIN" },
  ViewOrganizationSettings: { resourceType: "ORGANIZATION", role: "READER" },
  ViewLicenseDetails: { resourceType: "WORKSPACE", role: "READER" },
  CreateConfigTemplate: { resourceType: "ORGANIZATION", role: "ADMIN" },
  DownloadDiagnostics: { resourceType: "ORGANIZATION", role: "READER" },
  ViewOrganizationWorkspaces: { resourceType: "ORGANIZATION", role: "READER" },
  CreateOrganizationWorkspaces: { resourceType: "ORGANIZATION", role: "EDITOR" },

  // workspace
  DeleteWorkspace: { resourceType: "WORKSPACE", role: "ADMIN" },
  UpdateWorkspace: [
    { resourceType: "WORKSPACE", role: "ADMIN" },
    { resourceType: "ORGANIZATION", role: "EDITOR" },
  ],
  UpdateWorkspacePermissions: { resourceType: "WORKSPACE", role: "ADMIN" },
  UploadCustomConnector: { resourceType: "WORKSPACE", role: "EDITOR" },
  ViewWorkspaceSettings: { resourceType: "WORKSPACE", role: "READER" },

  // builder
  CreateCustomConnector: { resourceType: "WORKSPACE", role: "EDITOR" },
  UpdateCustomConnector: { resourceType: "WORKSPACE", role: "EDITOR" },

  // connection
  CreateConnection: { resourceType: "WORKSPACE", role: "EDITOR" },
  EditConnection: { resourceType: "WORKSPACE", role: "EDITOR" },
  ClearData: { resourceType: "WORKSPACE", role: "EDITOR" },
} as const;
