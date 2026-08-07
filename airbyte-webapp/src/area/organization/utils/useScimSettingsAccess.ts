import { useGetScimConfig } from "core/api";
import { ScimConfigResponse } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";
import { Intent } from "core/utils/rbac/generated-intents";
import { useGeneratedIntent } from "core/utils/rbac/useGeneratedIntent";

import { useCurrentOrganizationId } from "./useCurrentOrganizationId";

export interface ScimSettingsAccess {
  /**
   * settings.scimProvisioning flag AND org context AND org-admin intent.
   * Gates rendering of every SCIM config surface AND the getScimConfig request itself,
   * so non-admins (who reach the settings pages via ViewOrganizationSettings) never
   * issue a request the ORGANIZATION_ADMIN-secured endpoint would 403.
   */
  canManageScim: boolean;
  /**
   * Server-reported availability (entitlements + backend pilot flag), from the config
   * response. Gates functional actions (Enable / Rotate / Disable). false until loaded.
   */
  isScimAvailable: boolean;
  /** Loaded config; undefined while loading or whenever canManageScim is false. */
  scimConfig: ScimConfigResponse | undefined;
  /** True only while canManageScim and the config request is in flight. */
  isLoading: boolean;
  /** True only when canManageScim and the getScimConfig request has failed. */
  isError: boolean;
}

/**
 * Single access gate for the admin SCIM settings surfaces.
 * NOTE: members-page SCIM state must NOT use this hook — it reads the member-readable
 * `scim` field on OrganizationInfoRead instead, so that non-admins never trigger a
 * getScimConfig request.
 * Chrome (nav label, headings) gates on useExperiment("settings.scimProvisioning") alone.
 */
export const useScimSettingsAccess = (): ScimSettingsAccess => {
  const organizationId = useCurrentOrganizationId();
  const isScimFlagEnabled = useExperiment("settings.scimProvisioning");
  // UpdateOrganizationPermissions is the generated intent whose allow-list
  // (organization_admin, instance_admin) exactly matches the ORGANIZATION_ADMIN
  // security on the four scim_config endpoints. No SCIM-specific intent exists yet.
  const isOrgAdmin = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });

  const canManageScim = isScimFlagEnabled && !!organizationId && isOrgAdmin;
  const { data, isInitialLoading, isError } = useGetScimConfig({ enabled: canManageScim });

  // Guard against stale cache data surviving a role/flag change mid-session.
  const scimConfig = canManageScim ? data : undefined;

  return {
    canManageScim,
    isScimAvailable: scimConfig?.available === true,
    scimConfig,
    isLoading: canManageScim && isInitialLoading,
    isError: canManageScim && isError,
  };
};
