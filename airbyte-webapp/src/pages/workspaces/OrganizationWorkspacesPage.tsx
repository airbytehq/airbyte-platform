import capitalize from "lodash/capitalize";
import React, { useState, useCallback, useRef, useMemo, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";
import { Virtuoso, VirtuosoHandle } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ListBox } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useListUsersInOrganization,
  useListWorkspacesInOrganization,
  useGetWorkspacesStatusesCounts,
  useOrgInfo,
} from "core/api";
import { WorkspaceRead, WebBackendConnectionStatusCounts } from "core/api/types/AirbyteClient";
import { useWebappConfig } from "core/config";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import OrganizationWorkspaceItem from "./components/OrganizationWorkspaceItem";
import { OrganizationWorkspacesCreateControl } from "./components/OrganizationWorkspacesCreateControl";
import styles from "./OrganizationWorkspacesPage.module.scss";

type StatusFilter = "all" | "running" | "healthy" | "paused" | "failed";

const OrganizationWorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const { formatMessage } = useIntl();
  const { edition } = useWebappConfig();

  const organizationId = useCurrentOrganizationId();
  const organization = useOrgInfo(organizationId);
  const { isUnifiedTrialPlan } = useOrganizationSubscriptionStatus();
  const canViewOrganizationDetails = useGeneratedIntent(Intent.ViewOrganizationDetails);
  const memberCount = useListUsersInOrganization(canViewOrganizationDetails ? organizationId : undefined).users.length;

  const [searchValue, setSearchValue] = useState("");
  const [debouncedSearchValue, setDebouncedSearchValue] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");

  const { data, isLoading, hasNextPage, fetchNextPage, isFetchingNextPage, refetch } = useListWorkspacesInOrganization({
    organizationId,
    nameContains: debouncedSearchValue,
    pagination: {
      pageSize: 10,
    },
  });

  // This query is only used to check if the org has more than one workspace, which "grandfathers" the org into
  // multi-workspace mode.  It intentionally uses the same default parameters as the query above so that only one fetch
  // request fires when this page is loaded.
  const { data: moreThanOneWorkspaceData } = useListWorkspacesInOrganization({
    organizationId,
    nameContains: "",
    pagination: {
      pageSize: 10,
    },
  });

  const workspaces = useMemo(() => data?.pages.flatMap((page) => page.workspaces) ?? [], [data]);
  const workspaceIds = workspaces.map((workspace) => workspace.workspaceId);

  const firstPageOfWorkspaces = moreThanOneWorkspaceData?.pages?.[0]?.workspaces ?? [];
  const hasCreateWorkspacePermission = useIntent("CreateOrganizationWorkspaces", { organizationId });
  const hasCreateMultipleWorkspacesFeature = useFeature(FeatureItem.CreateMultipleWorkspaces);

  const hasNoWorkspaces = !isLoading && firstPageOfWorkspaces.length === 0;
  const isCreateWorkspaceEnabled =
    hasCreateWorkspacePermission && (hasNoWorkspaces || hasCreateMultipleWorkspacesFeature);

  // Get status counts for all workspaces
  const statusCountsResults = useGetWorkspacesStatusesCounts(workspaceIds);

  const enrichedWorkspaces = useMemo(() => {
    const statusCountsMap = new Map<string, WebBackendConnectionStatusCounts | undefined>();
    statusCountsResults.forEach((result) => {
      const workspaceId = result.data?.workspaceId;
      if (workspaceId && result.data) {
        statusCountsMap.set(workspaceId, result.data.statusCounts);
      }
    });
    return workspaces.map((workspace) => {
      const statusCounts = statusCountsMap.get(workspace.workspaceId);
      return { ...workspace, statusCounts };
    });
  }, [workspaces, statusCountsResults]);

  const filteredWorkspaces = useMemo(() => {
    if (statusFilter === "all") {
      return enrichedWorkspaces;
    }

    return enrichedWorkspaces.filter((workspace) => (workspace.statusCounts?.[statusFilter] ?? 0) > 0);
  }, [enrichedWorkspaces, statusFilter]);

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  useDebounce(
    () => {
      setDebouncedSearchValue(searchValue);
      virtuosoRef.current?.scrollTo({ top: 0 });
    },
    250,
    [searchValue]
  );

  const handleEndReached = useCallback(() => {
    if (hasNextPage) {
      fetchNextPage();
    }
  }, [hasNextPage, fetchNextPage]);

  const statusFilterOptions = [
    { label: "All sync statuses", value: "all" as StatusFilter },
    { label: "Successful syncs", value: "healthy" as StatusFilter },
    { label: "Running syncs", value: "running" as StatusFilter },
    { label: "Incomplete syncs", value: "paused" as StatusFilter },
    { label: "Failed syncs", value: "failed" as StatusFilter },
  ];

  useEffect(() => {
    setStatusFilter("all");
  }, [organizationId]);

  const showNoWorkspacesYet =
    filteredWorkspaces.length === 0 && !isLoading && debouncedSearchValue === "" && statusFilter === "all";

  const showNoWorkspacesFound = filteredWorkspaces.length === 0 && !isLoading;

  return (
    <div className={styles.background}>
      <FlexContainer direction="column" alignItems="center" className={styles.contentContainer}>
        <FlexContainer direction="column" gap="md" className={styles.content}>
          <FlexContainer justifyContent="space-between" alignItems="center" className={styles.headerRow}>
            <Box>
              <Heading as="h1" size="lg">
                {organization?.organizationName || formatMessage({ id: "organization.emptyName" })}
              </Heading>
              {canViewOrganizationDetails && (
                <Text color="grey400" size="sm">
                  {formatMessage(
                    { id: "organization.members" },
                    { subscriptionName: capitalize(edition), count: memberCount }
                  )}
                </Text>
              )}
            </Box>
            <FlexContainer alignItems="center" gap="sm">
              {isUnifiedTrialPlan && <BrandingBadge product="cloudForTeams" />}
              <OrganizationWorkspacesCreateControl disabled={!isCreateWorkspaceEnabled} onCreated={refetch} />
            </FlexContainer>
          </FlexContainer>
          <Box>
            <SearchInput value={searchValue} onChange={setSearchValue} data-testid="workspaces-page-search" />
          </Box>
          <Box>
            <ListBox
              options={statusFilterOptions}
              selectedValue={statusFilter}
              onSelect={setStatusFilter}
              placeholder={formatMessage({ id: "workspaces.statusFilter.placeholder" })}
              data-testid="workspaces-status-filter"
              buttonClassName={styles.statusFilterButton}
            />
          </Box>
          <Box className={styles.workspacesList}>
            {isLoading ? (
              <Box p="md" pb="sm">
                <LoadingSpinner />
              </Box>
            ) : showNoWorkspacesYet ? (
              <FlexContainer direction="column" alignItems="center" justifyContent="flex-start">
                <Box mb="sm" mt="xl">
                  <FlexContainer
                    alignItems="center"
                    justifyContent="center"
                    className={styles.emptyStateIconBackground}
                  >
                    <Icon type="grid" size="xl" className={styles.emptyStateIcon} />
                  </FlexContainer>
                </Box>
                <Box mb="sm">
                  <Text size="md" color="grey500">
                    <FormattedMessage id="workspaces.noWorkspacesYet" />
                  </Text>
                </Box>
                <OrganizationWorkspacesCreateControl
                  disabled={!isCreateWorkspaceEnabled}
                  secondary
                  onCreated={refetch}
                />
              </FlexContainer>
            ) : showNoWorkspacesFound ? (
              <Box mt="xl">
                <FlexContainer direction="column" alignItems="center" justifyContent="flex-start">
                  <Text size="md">
                    <FormattedMessage id="workspaces.noWorkspaces" />
                  </Text>
                </FlexContainer>
              </Box>
            ) : (
              <Virtuoso
                ref={virtuosoRef}
                style={{ height: "100%" }}
                data={filteredWorkspaces}
                endReached={handleEndReached}
                computeItemKey={(index, item) => item.workspaceId + index}
                itemContent={(
                  _: number,
                  workspace: WorkspaceRead & { statusCounts: WebBackendConnectionStatusCounts | undefined }
                ) => <OrganizationWorkspaceItem key={workspace.workspaceId} workspace={workspace} />}
                components={{
                  Footer: isFetchingNextPage
                    ? () => (
                        <Box pt="md" pb="sm" className={styles.footerLoading}>
                          <LoadingSpinner />
                        </Box>
                      )
                    : () => <Box pb="2xl" />,
                }}
              />
            )}
          </Box>
        </FlexContainer>
      </FlexContainer>
    </div>
  );
};

export default OrganizationWorkspacesPage;
