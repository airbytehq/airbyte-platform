import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";

import { GuestBadge } from "./GuestBadge";
import { RoleManagementMenu } from "./RoleManagementMenu";
import {
  ResourceType,
  UnifiedWorkspaceUserModel,
  getWorkspaceAccessLevel,
} from "../components/useGetAccessManagementData";
import { UserRoleText } from "../components/UserRoleText";

const ViewOnlyRoleBox: React.FC<{ highestPermissionType: "MEMBER" | "ADMIN" | "READER" | "EDITOR" }> = ({
  highestPermissionType,
}) => {
  return (
    <Box pl="md" pr="sm">
      <FlexContainer gap="md" alignItems="center" justifyContent="flex-start">
        <UserRoleText highestPermissionType={highestPermissionType} />
      </FlexContainer>
    </Box>
  );
};

interface RoleManagementCellProps {
  user: UnifiedWorkspaceUserModel;
  resourceType: ResourceType;
}

export const RoleManagementCell: React.FC<RoleManagementCellProps> = ({ user, resourceType }) => {
  const { workspaceId } = useCurrentWorkspace();
  const organizationInfo = useCurrentOrganizationInfo();
  const highestPermissionType = getWorkspaceAccessLevel(user);
  const currentUser = useCurrentUser();
  const orgPermissionType = user.organizationPermission ? user.organizationPermission.permissionType : undefined;
  const canEditPermissions = useIntent(
    resourceType === "workspace" ? "UpdateWorkspacePermissions" : "UpdateOrganizationPermissions",
    { workspaceId, organizationId: organizationInfo?.organizationId }
  );
  const canListOrganizationUsers = useIntent("ListOrganizationMembers", {
    organizationId: organizationInfo?.organizationId,
  });
  const indicateGuestUsers = useFeature(FeatureItem.IndicateGuestUsers);
  const cannotDemoteUser = resourceType === "workspace" && orgPermissionType === "organization_admin";
  const shouldHidePopover = cannotDemoteUser || !canEditPermissions || user.id === currentUser.userId;

  const tooltipContent =
    cannotDemoteUser && canEditPermissions
      ? "settings.accessManagement.cannotDemoteOrgAdmin"
      : user.id === currentUser.userId && canEditPermissions
      ? "settings.accessManagement.cannotEditOwnPermissions"
      : undefined;

  return (
    <FlexContainer gap="xs" alignItems="center">
      {shouldHidePopover ? (
        tooltipContent ? (
          <Tooltip control={<ViewOnlyRoleBox highestPermissionType={highestPermissionType} />}>
            <FormattedMessage id={tooltipContent} />
          </Tooltip>
        ) : (
          <ViewOnlyRoleBox highestPermissionType={highestPermissionType} />
        )
      ) : (
        <RoleManagementMenu user={user} highestPermissionType={highestPermissionType} resourceType={resourceType} />
      )}
      {user.organizationPermission?.permissionType === "organization_admin" && resourceType === "workspace" && (
        <Badge variant="grey">
          <FormattedMessage id="role.organizationAdmin" />
        </Badge>
      )}
      {canListOrganizationUsers && organizationInfo?.organizationId && indicateGuestUsers && (
        <GuestBadge userId={user.id} organizationId={organizationInfo.organizationId} />
      )}
      {user.invitationStatus === "pending" && (
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
            <FormattedMessage id="userInvitations.pendingInvitation.tooltipAdditionalInfo" />
          </Text>
        </Tooltip>
      )}
    </FlexContainer>
  );
};
