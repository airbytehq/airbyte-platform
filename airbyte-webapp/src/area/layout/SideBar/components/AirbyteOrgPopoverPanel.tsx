import React, { useState, useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { useListOrganizationsByUser, useListWorkspacesByUser } from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth/AuthContext";
import { useFeature, FeatureItem } from "core/services/features";

import styles from "./AirbyteOrgPopoverPanel.module.scss";
import { OrganizationWithWorkspaces } from "./OrganizationWithWorkspaces";

export const AirbyteOrgPopoverPanel: React.FC = () => {
  const { formatMessage } = useIntl();
  const allowMultiWorkspace = useFeature(FeatureItem.MultiWorkspaceUI);

  const { userId } = useCurrentUser();
  const { organizations } = useListOrganizationsByUser({ userId });
  const [search, setSearch] = useState("");
  const { data: workspaces = [] } = useListWorkspacesByUser(search);

  // TODO: Incorporate new endpoint and add error handling and loading state when this PR is merged
  // https://github.com/airbytehq/airbyte-platform-internal/pull/16751

  const orgsWithWorkspaces = useMemo(() => {
    const workspaceMap = new Map<string, WorkspaceRead[]>();
    workspaces.forEach((ws) => {
      const existing = workspaceMap.get(ws.organizationId) || [];
      workspaceMap.set(ws.organizationId, [...existing, ws]);
    });

    return organizations
      .map((org) => ({
        ...org,
        workspaces: workspaceMap.get(org.organizationId) || [],
      }))
      .filter((org) => org.workspaces.length > 0 || org.organizationName.toLowerCase().includes(search.toLowerCase()));
  }, [organizations, workspaces, search]);

  const handleSearchChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setSearch(e.target.value);
  }, []);

  return (
    <div>
      {allowMultiWorkspace && (
        <Box p="md" className={styles.searchInputBox}>
          <Input
            type="text"
            name="search"
            placeholder={formatMessage({ id: "sidebar.searchAllWorkspaces", defaultMessage: "Search all workspaces" })}
            value={search}
            onChange={handleSearchChange}
            className={styles.searchInput}
            containerClassName={styles.searchInputContainer}
            aria-label={formatMessage({ id: "sidebar.searchAllWorkspaces", defaultMessage: "Search all workspaces" })}
          />
        </Box>
      )}
      <FlexContainer direction="column" gap="xs" className={styles.list}>
        {orgsWithWorkspaces.length === 0 ? (
          <Box p="md">
            <Text align="center">
              <FormattedMessage id="workspaces.noWorkspaces" />
            </Text>
          </Box>
        ) : (
          orgsWithWorkspaces.map((org) => (
            <OrganizationWithWorkspaces key={org.organizationId} organization={org} workspaces={org.workspaces ?? []} />
          ))
        )}
      </FlexContainer>
    </div>
  );
};
