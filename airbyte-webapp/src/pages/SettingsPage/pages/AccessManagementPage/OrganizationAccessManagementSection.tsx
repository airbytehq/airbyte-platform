import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import {
  useCurrentOrganizationInfo,
  useCurrentWorkspace,
  useListUserInvitations,
  useListUsersInOrganization,
} from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

import { AddUserModal } from "./components/AddUserModal";
import { UnifiedUserModel, unifyOrganizationUserData } from "./components/util";
import styles from "./OrganizationAccessManagementSection.module.scss";
import { OrganizationUsersTable } from "./OrganizationUsersTable";

const SEARCH_PARAM = "search";

export const OrganizationAccessManagementSection: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const workspace = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();
  const canUpdateOrganizationPermissions = useIntent("UpdateOrganizationPermissions", {
    organizationId: organization.organizationId,
  });
  const allowExternalInvitations = useFeature(FeatureItem.ExternalInvitations);

  const { openModal } = useModalService();

  const { users } = useListUsersInOrganization(workspace.organizationId);

  const pendingInvitations = useListUserInvitations({
    scopeType: "organization",
    scopeId: organization.organizationId,
  });

  const unifiedOrganizationUsers = unifyOrganizationUserData(users, pendingInvitations);

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get("search");
  const [userFilter, setUserFilter] = React.useState(filterParam ?? "");
  const debouncedUserFilter = useDeferredValue(userFilter);
  const { formatMessage } = useIntl();
  const allowOrganizationInvites = useExperiment("settings.organizationRbacImprovements");
  const showInviteUsers = !organization?.sso && allowExternalInvitations && allowOrganizationInvites;

  const onOpenInviteUsersModal = () =>
    openModal<void>({
      title: formatMessage({ id: "userInvitations.create.modal.title" }, { scopeName: organization.organizationName }),
      content: ({ onComplete }) => <AddUserModal onSubmit={onComplete} scope="organization" />,
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

  const filteredOrganizationUsers: UnifiedUserModel[] = unifiedOrganizationUsers.filter((user) => {
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
          <SearchInput value={userFilter} onChange={(e) => setUserFilter(e.target.value)} />
        </FlexItem>
        <FlexContainer alignItems="baseline">
          {organization?.sso && (
            <Badge variant="blue">
              <FlexContainer gap="xs" alignItems="center">
                <Icon type="check" size="xs" />
                <Text size="sm">
                  <FormattedMessage id="settings.accessManagement.ssoEnabled" />
                </Text>
              </FlexContainer>
            </Badge>
          )}
          {!organization?.sso && isCloudApp && (
            <ExternalLink href={links.contactSales}>
              <Text size="sm" color="blue">
                <FormattedMessage id="settings.accessManagement.enableSso" />
              </Text>
            </ExternalLink>
          )}

          {showInviteUsers && (
            <Button onClick={onOpenInviteUsersModal} disabled={!canUpdateOrganizationPermissions} icon="plus">
              <FormattedMessage id="userInvitations.newMember" />
            </Button>
          )}
        </FlexContainer>
      </FlexContainer>
      {filteredOrganizationUsers && filteredOrganizationUsers.length > 0 ? (
        <OrganizationUsersTable users={filteredOrganizationUsers} />
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
