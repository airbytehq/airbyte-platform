import classNames from "classnames";
import React, { useState, useCallback, useRef, useMemo, Suspense } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce, useLocation, useUpdateEffect } from "react-use";
import { Virtuoso, VirtuosoHandle } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { BrandingBadge, useGetProductBranding } from "components/ui/BrandingBadge";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useListOrganizationSummaries,
  useListWorkspacesInOrganization,
  useOrganization,
  useOrganizationUserCount,
} from "core/api";
import { OrganizationSummary } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth/AuthContext";
import { useFeature, FeatureItem } from "core/services/features";

import styles from "./AirbyteOrgPopoverPanel.module.scss";
import { OrganizationWithWorkspaces, WorkspaceNavLink } from "./OrganizationWithWorkspaces";

const SINGLE_WORKSPACE_HEIGHT = 64;
const ORG_ITEM_HEIGHT = 50;
const WORKSPACE_ITEM_HEIGHT = 32;
const MAX_HEIGHT = 388;

const OrganizationSummariesList: React.FC<{
  debouncedSearchValue: string;
  virtuosoRef: React.RefObject<VirtuosoHandle>;
}> = ({ debouncedSearchValue, virtuosoRef }) => {
  const allowMultiWorkspace = useFeature(FeatureItem.MultiWorkspaceUI);
  const showOSSWorkspaceName = useFeature(FeatureItem.ShowOSSWorkspaceName);
  const { formatMessage } = useIntl();
  const { userId } = useCurrentUser();
  const currentOrganizationId = useCurrentOrganizationId();
  const currentOrganization = useOrganization(currentOrganizationId);
  const currentOrganizationMemberCount = useOrganizationUserCount(currentOrganizationId);
  const product = useGetProductBranding();

  const { data, isLoading, isFetchingNextPage, hasNextPage, fetchNextPage } = useListOrganizationSummaries({
    userId,
    nameContains: debouncedSearchValue,
    pagination: {
      pageSize: 10,
    },
  });

  const organizationSummaries: Array<OrganizationSummary & { brandingBadge?: React.ReactNode }> = useMemo(() => {
    const summaries: Array<OrganizationSummary & { brandingBadge?: React.ReactNode }> =
      data?.pages.flatMap((page) => {
        if (showOSSWorkspaceName && page.organizationSummaries?.[0]?.workspaces?.[0]) {
          page.organizationSummaries[0].workspaces[0].name = formatMessage({ id: "sidebar.myWorkspace" });
        }
        return page.organizationSummaries ?? [];
      }) ?? [];

    return summaries;
  }, [data?.pages, showOSSWorkspaceName, formatMessage]);

  const {
    data: workspacesData,
    isFetchingNextPage: isFetchingNextWorkspacesPage,
    hasNextPage: hasNextWorkspacesPage,
    fetchNextPage: fetchNextWorkspacesPage,
  } = useListWorkspacesInOrganization({
    organizationId: currentOrganizationId,
    nameContains: "",
    pagination: {
      pageSize: 10,
    },
  });

  const orgWorkspaces = useMemo(() => workspacesData?.pages.flatMap((page) => page.workspaces) ?? [], [workspacesData]);

  const estimatedHeight = !allowMultiWorkspace
    ? SINGLE_WORKSPACE_HEIGHT
    : !debouncedSearchValue
    ? Math.min(ORG_ITEM_HEIGHT + WORKSPACE_ITEM_HEIGHT * orgWorkspaces.length, MAX_HEIGHT)
    : Math.min(
        organizationSummaries.reduce((acc, curr) => {
          return acc + ORG_ITEM_HEIGHT + (curr?.workspaces?.length || 0) * WORKSPACE_ITEM_HEIGHT;
        }, 0),
        MAX_HEIGHT
      );

  const handleEndReached = () => {
    if (hasNextPage) {
      fetchNextPage();
    }
  };

  const handleWorkspacesEndReached = () => {
    if (hasNextWorkspacesPage) {
      fetchNextWorkspacesPage();
    }
  };

  if (isLoading) {
    return (
      <Box p="md" pb="sm">
        <Text align="center">
          <LoadingSpinner />
        </Text>
      </Box>
    );
  }

  if (organizationSummaries?.length === 0) {
    return (
      <Box p="md">
        <Text align="center">
          <FormattedMessage id="workspaces.noWorkspaces" />
        </Text>
      </Box>
    );
  }

  // Only show the current organization when no search is active.
  if (allowMultiWorkspace && debouncedSearchValue === "") {
    return (
      <Virtuoso
        ref={virtuosoRef}
        style={{ height: estimatedHeight }}
        data={orgWorkspaces}
        endReached={handleWorkspacesEndReached}
        computeItemKey={(index, item) => item.workspaceId + index}
        itemContent={(_index, item) => <WorkspaceNavLink workspaceId={item.workspaceId} name={item.name} />}
        components={{
          Header: () => (
            <OrganizationWithWorkspaces
              organization={currentOrganization}
              brandingBadge={
                <>
                  <BrandingBadge product={product} testId={`${product}-badge`} />{" "}
                </>
              }
              workspaces={[]}
              memberCount={currentOrganizationMemberCount ?? 0}
              lastItem
            />
          ),
          Footer: isFetchingNextWorkspacesPage
            ? () => (
                <Box pt="md" pb="sm" className={styles.footerLoading}>
                  <LoadingSpinner />
                </Box>
              )
            : undefined,
        }}
      />
    );
  }

  return (
    <Virtuoso
      ref={virtuosoRef}
      style={{ height: estimatedHeight }}
      data={organizationSummaries}
      endReached={handleEndReached}
      computeItemKey={(index, item) => item.organization.organizationId + index}
      itemContent={(index, item) => (
        <OrganizationWithWorkspaces {...item} lastItem={index === organizationSummaries.length - 1} />
      )}
      components={{
        Footer: isFetchingNextPage
          ? () => (
              <Box pt="md" pb="sm" className={styles.footerLoading}>
                <LoadingSpinner />
              </Box>
            )
          : undefined,
      }}
    />
  );
};

export const AirbyteOrgPopoverPanel: React.FC<{ closePopover: () => void }> = ({ closePopover }) => {
  const location = useLocation();
  const { formatMessage } = useIntl();
  const allowMultiWorkspace = useFeature(FeatureItem.MultiWorkspaceUI);
  const [search, setSearch] = useState("");
  const [debouncedSearchValue, setDebouncedSearchValue] = useState("");

  const handleSearchChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setSearch(e.target.value);
  }, []);

  useDebounce(
    () => {
      setDebouncedSearchValue(search);
      virtuosoRef.current?.scrollTo({ top: 0 });
    },
    250,
    [search]
  );

  useUpdateEffect(() => {
    closePopover();
  }, [closePopover, location.pathname, location.search]);

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  return (
    <>
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
      <FlexContainer
        direction="column"
        gap="xs"
        className={classNames(styles.list, { [styles["list--singleWorkspace"]]: !allowMultiWorkspace })}
        style={{ overflow: "auto" }}
      >
        <Suspense
          fallback={
            <Box p="md" pb="sm">
              <LoadingSpinner />
            </Box>
          }
        >
          <OrganizationSummariesList debouncedSearchValue={debouncedSearchValue} virtuosoRef={virtuosoRef} />
        </Suspense>
      </FlexContainer>
    </>
  );
};
