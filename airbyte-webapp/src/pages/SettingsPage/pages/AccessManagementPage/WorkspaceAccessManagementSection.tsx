import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace, useListWorkspaceAccessUsers } from "core/api";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { AddUserModal } from "packages/cloud/views/workspaces/WorkspaceSettingsView/components/AddUserModal";
import { FirebaseInviteUserButton } from "packages/cloud/views/workspaces/WorkspaceSettingsView/components/FirebaseInviteUserButton";

import { AddUserControl } from "./components/AddUserControl";
import styles from "./WorkspaceAccessManagementSection.module.scss";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const SEARCH_PARAM = "search";

const WorkspaceAccessManagementSection: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();
  const canViewOrgMembers = useIntent("ListOrganizationMembers", { organizationId: organization?.organizationId });
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId: workspace.workspaceId });
  const { openModal, closeModal } = useModalService();

  const usersWithAccess = useListWorkspaceAccessUsers(workspace.workspaceId).usersWithAccess;

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get("search");
  const [userFilter, setUserFilter] = React.useState(filterParam ?? "");
  const debouncedUserFilter = useDeferredValue(userFilter);
  const { formatMessage } = useIntl();

  const showAddUserButton = organization?.sso && canUpdateWorkspacePermissions && canViewOrgMembers;
  const showFirebaseInviteButton = !organization?.sso && canUpdateWorkspacePermissions;
  const invitationSystemv2 = useExperiment("settings.invitationSystemv2", false);

  const onOpenInviteUsersModal = () =>
    openModal({
      title: formatMessage({ id: "userInvitations.create.modal.title" }, { workspace: workspace.name }),
      content: () => <AddUserModal closeModal={closeModal} />,
      size: "md",
    });

  useEffect(() => {
    if (debouncedUserFilter) {
      searchParams.set(SEARCH_PARAM, debouncedUserFilter);
    } else {
      searchParams.delete(SEARCH_PARAM);
    }
    setSearchParams(searchParams);
  }, [debouncedUserFilter, searchParams, setSearchParams]);

  const filteredUsersWithAccess = (usersWithAccess ?? []).filter((user) => {
    return (
      user.userName?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.userEmail?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
    );
  });

  return (
    <FlexContainer direction="column" gap="md">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Text size="lg">
          <FormattedMessage id="settings.accessManagement.members" />
        </Text>
      </FlexContainer>
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexItem className={styles.searchInputWrapper}>
          <SearchInput value={userFilter} onChange={(e) => setUserFilter(e.target.value)} />
        </FlexItem>
        {!invitationSystemv2 ? (
          <>
            {showFirebaseInviteButton && <FirebaseInviteUserButton />}
            {showAddUserButton && <AddUserControl />}
          </>
        ) : (
          <Button
            onClick={onOpenInviteUsersModal}
            disabled={!canUpdateWorkspacePermissions}
            icon={<Icon type="plus" />}
          >
            <FormattedMessage id="userInvitations.newMember" />
          </Button>
        )}
      </FlexContainer>
      {filteredUsersWithAccess && filteredUsersWithAccess.length > 0 ? (
        <WorkspaceUsersTable users={filteredUsersWithAccess} />
      ) : (
        <Box py="xl" pl="lg">
          <Text color="grey" italicized>
            <FormattedMessage id="settings.accessManagement.noUsers" />
          </Text>
        </Box>
      )}
    </FlexContainer>
  );
};

export default WorkspaceAccessManagementSection;
