export const intentToRbacQuery = {
  // instance

  // organization
  ListOrganizationMembers: { resourceType: "ORGANIZATION", role: "MEMBER" },
  UpdateConnectorVersions: { resourceType: "ORGANIZATION", role: "ADMIN" }, // this setting is currently instance-wide, but we've decided to scope to organization permissions for now
  UpdateOrganization: { resourceType: "ORGANIZATION", role: "ADMIN" },
  ViewOrganizationSettings: { resourceType: "ORGANIZATION", role: "READER" },
  ViewLicenseDetails: { resourceType: "WORKSPACE", role: "READER" },
  DownloadDiagnostics: { resourceType: "INSTANCE", role: "ADMIN" },
  ViewOrganizationWorkspaces: { resourceType: "ORGANIZATION", role: "READER" },
  CreateOrganizationWorkspaces: { resourceType: "ORGANIZATION", role: "EDITOR" },

  // workspace
  EditConnectionSemanticSearch: { resourceType: "WORKSPACE", role: "EDITOR" }, // matches the WORKSPACE_EDITOR check on the connection enablement endpoints
} as const;
