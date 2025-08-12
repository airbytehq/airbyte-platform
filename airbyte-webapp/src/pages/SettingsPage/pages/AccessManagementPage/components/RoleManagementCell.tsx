import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

import { GuestBadge } from "./GuestBadge";
import { RoleManagementMenu } from "./RoleManagementMenu";
import { UserRoleText } from "./UserRoleText";
import { ResourceType, UnifiedUserModel, getOrganizationAccessLevel, getWorkspaceAccessLevel } from "./util";

const ViewOnlyRoleBox: React.FC<{ highestPermissionType: RbacRole }> = ({ highestPermissionType }) => {
  return (
    <Box pl="md" pr="sm">
      <FlexContainer gap="md" alignItems="center" justifyContent="flex-start">
        <UserRoleText highestPermissionType={highestPermissionType} />
      </FlexContainer>
    </Box>
  );
};

interface RoleManagementCellProps {
  user: UnifiedUserModel;
  resourceType: ResourceType;
}

export const PendingInvitationBadge: React.FC<{ scope: ResourceType }> = ({ scope }) => {
  return (
    <Tooltip
      control={
        <Box px="sm">
          <Text italicized color="grey400">
            <FormattedMessage id="userInvitations.pendingInvitation" />
          </Text>
        </Box>
      }
    >
      <Text bold italicized inverseColor>
        <FormattedMessage id="userInvitations.pendingInvitation.tooltipMain" />
      </Text>
      <Text italicized inverseColor>
        <FormattedMessage
          id="userInvitations.pendingInvitation.tooltipAdditionalInfo"
          values={{
            scope,
          }}
        />
      </Text>
    </Tooltip>
  );
};
export const RoleManagementCell: React.FC<RoleManagementCellProps> = ({ user, resourceType }) => {
  const indicateGuestUsers = useFeature(FeatureItem.IndicateGuestUsers);
  const organizationId = useCurrentOrganizationId();
  const workspaceAccessLevel = getWorkspaceAccessLevel(user);
  const organizationAccessLevel = getOrganizationAccessLevel(user);
  const currentUser = useCurrentUser();

  const canEditWorkspacePermissions = useGeneratedIntent(Intent.UpdateWorkspacePermissions);
  const canEditOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions);
  const canEditPermissions =
    resourceType === "workspace" ? canEditWorkspacePermissions : canEditOrganizationPermissions;

  const canListOrganizationUsers = useIntent("ListOrganizationMembers", {
    organizationId,
  });
  const cannotDemoteUser =
    resourceType === "workspace" && organizationAccessLevel === "ADMIN" && user.invitationStatus === undefined;

  const showViewOnlyBox = cannotDemoteUser || !canEditPermissions || user.id === currentUser.userId;

  const tooltipContent =
    cannotDemoteUser && canEditPermissions
      ? "settings.accessManagement.cannotDemoteOrgAdmin"
      : user.id === currentUser.userId && canEditPermissions
      ? "settings.accessManagement.cannotEditOwnPermissions"
      : undefined;
  return (
    <FlexContainer gap="xs" alignItems="center">
      {showViewOnlyBox ? (
        tooltipContent ? (
          <Tooltip control={<ViewOnlyRoleBox highestPermissionType={workspaceAccessLevel} />}>
            <FormattedMessage id={tooltipContent} />
          </Tooltip>
        ) : (
          <ViewOnlyRoleBox highestPermissionType={workspaceAccessLevel} />
        )
      ) : (
        <RoleManagementMenu
          user={user}
          highestPermissionType={resourceType === "workspace" ? workspaceAccessLevel : organizationAccessLevel}
          resourceType={resourceType}
        />
      )}
      {user.organizationPermission?.permissionType === "organization_admin" && resourceType === "workspace" && (
        <Badge variant="grey">
          <FormattedMessage id="role.organizationAdmin" />
        </Badge>
      )}
      {canListOrganizationUsers && indicateGuestUsers && (
        <GuestBadge userId={user.id} organizationId={organizationId} />
      )}
      {user.invitationStatus === "pending" && <PendingInvitationBadge scope={resourceType} />}
    </FlexContainer>
  );
};
