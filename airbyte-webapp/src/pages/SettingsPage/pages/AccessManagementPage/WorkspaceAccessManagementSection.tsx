import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace, useListUserInvitations, useListWorkspaceAccessUsers } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import { AddUserModal } from "./components/AddUserModal";
import { UnifiedUserModel, unifyWorkspaceUserData } from "./components/util";
import styles from "./WorkspaceAccessManagementSection.module.scss";
import { WorkspaceUsersTable } from "./WorkspaceUsersTable";

const SEARCH_PARAM = "search";

const WorkspaceAccessManagementSection: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const canUpdateWorkspacePermissions = useGeneratedIntent(Intent.UpdateWorkspacePermissions);
  const { openModal } = useModalService();

  const { usersWithAccess } = useListWorkspaceAccessUsers(workspace.workspaceId);

  const pendingInvitations = useListUserInvitations({
    scopeType: "workspace",
    scopeId: workspace.workspaceId,
  });
  const unifiedWorkspaceUsers = unifyWorkspaceUserData(usersWithAccess, pendingInvitations);

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get("search");
  const [userFilter, setUserFilter] = React.useState(filterParam ?? "");
  const debouncedUserFilter = useDeferredValue(userFilter);
  const { formatMessage } = useIntl();

  const onOpenInviteUsersModal = () =>
    openModal<void>({
      title: formatMessage({ id: "userInvitations.create.modal.title" }, { scopeName: workspace.name }),
      content: ({ onComplete }) => <AddUserModal onSubmit={onComplete} scope="workspace" />,
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

  const filteredWorkspaceUsers: UnifiedUserModel[] = (unifiedWorkspaceUsers ?? []).filter((user) => {
    return (
      user.userName?.toLowerCase().includes(filterParam?.toLowerCase() ?? "") ||
      user.userEmail?.toLowerCase().includes(filterParam?.toLowerCase() ?? "")
    );
  });

  return (
    <FlexContainer direction="column" gap="md">
      <FlexContainer justifyContent="space-between" alignItems="baseline">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.accessManagement.members" />
        </Heading>
      </FlexContainer>
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexItem className={styles.searchInputWrapper}>
          <SearchInput value={userFilter} onChange={setUserFilter} />
        </FlexItem>
        <Button onClick={onOpenInviteUsersModal} disabled={!canUpdateWorkspacePermissions} icon="plus">
          <FormattedMessage id="userInvitations.newMember" />
        </Button>
      </FlexContainer>
      {filteredWorkspaceUsers && filteredWorkspaceUsers.length > 0 ? (
        <WorkspaceUsersTable users={filteredWorkspaceUsers} />
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
