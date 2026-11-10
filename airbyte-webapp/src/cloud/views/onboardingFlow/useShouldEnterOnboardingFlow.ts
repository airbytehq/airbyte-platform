import { useCurrentOrganizationId, useIsAdpOrganization } from "area/organization/utils";
import { useCurrentOrganizationInfo, useListPermissions } from "core/api";
import { OrganizationOnboardingStatus, PermissionRead, PermissionType } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

const FINISHED_STATUSES: readonly OrganizationOnboardingStatus[] = ["completed", "skipped"];
const ORGANIZATION_EDITOR_ROLES: readonly PermissionType[] = ["organization_admin", "organization_editor"];

interface OnboardingFlowAccess {
  status?: OrganizationOnboardingStatus;
  canUpdateStatus: boolean;
  isAdpOrganization: boolean;
}

const isInProgress = (status?: OrganizationOnboardingStatus) =>
  status !== undefined && !FINISHED_STATUSES.includes(status);

/** ADP organizations never enter the flow. */
export const canOpenOnboardingFlow = ({ status, canUpdateStatus, isAdpOrganization }: OnboardingFlowAccess) =>
  isInProgress(status) && canUpdateStatus && !isAdpOrganization;

export const hasOrganizationEditorRole = (permissions: PermissionRead[], organizationId: string) =>
  permissions.some(
    ({ permissionType, organizationId: permissionOrganizationId }) =>
      permissionOrganizationId === organizationId && ORGANIZATION_EDITOR_ROLES.includes(permissionType)
  );

/**
 * Decides whether to redirect the user into the onboarding flow. Only users with an `organization_admin` or
 * `organization_editor` permission on this organization are redirected. Instance admins (Airbyte support staff) pass
 * the intent check but are not redirected: they open customer organizations to look at the normal app, and a redirect
 * would block that. They can still open the flow by going to its URL.
 */
export const shouldEnterOnboardingFlow = ({
  hasDirectEditorRole,
  ...access
}: OnboardingFlowAccess & { hasDirectEditorRole: boolean }) => canOpenOnboardingFlow(access) && hasDirectEditorRole;

const useOnboardingFlowAccess = (): OnboardingFlowAccess => ({
  status: useCurrentOrganizationInfo()?.onboardingStatus,
  canUpdateStatus: useGeneratedIntent(Intent.UpdateOrganizationOnboardingStatus),
  isAdpOrganization: useIsAdpOrganization(),
});

export const useCanOpenOnboardingFlow = () => canOpenOnboardingFlow(useOnboardingFlowAccess());

export const useShouldEnterOnboardingFlow = () => {
  const access = useOnboardingFlowAccess();
  const organizationId = useCurrentOrganizationId();
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);

  return shouldEnterOnboardingFlow({
    ...access,
    hasDirectEditorRole: hasOrganizationEditorRole(permissions, organizationId),
  });
};
