import classNames from "classnames";
import React, { useState, useCallback, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce, useLocation, useUpdateEffect } from "react-use";
import { Virtuoso, VirtuosoHandle } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useListOrganizationSummaries } from "core/api";
import { useCurrentUser } from "core/services/auth/AuthContext";
import { useFeature, FeatureItem } from "core/services/features";
import { isNonNullable } from "core/utils/isNonNullable";

import styles from "./AirbyteOrgPopoverPanel.module.scss";
import { OrganizationWithWorkspaces } from "./OrganizationWithWorkspaces";

export const AirbyteOrgPopoverPanel: React.FC<{ closePopover: () => void }> = ({ closePopover }) => {
  const location = useLocation();
  const { formatMessage } = useIntl();
  const allowMultiWorkspace = useFeature(FeatureItem.MultiWorkspaceUI);
  const showOSSWorkspaceName = useFeature(FeatureItem.ShowOSSWorkspaceName);

  const { userId } = useCurrentUser();
  const [search, setSearch] = useState("");
  const [debouncedSearchValue, setDebouncedSearchValue] = useState("");

  const { data, isLoading, isFetchingNextPage, hasNextPage, fetchNextPage } = useListOrganizationSummaries({
    userId,
    nameContains: debouncedSearchValue,
    pagination: {
      pageSize: 10,
    },
  });

  const organizationSummaries =
    data?.pages.flatMap((page) => {
      if (showOSSWorkspaceName && page.organizationSummaries?.[0]?.workspaces?.[0]) {
        page.organizationSummaries[0].workspaces[0].name = formatMessage({ id: "sidebar.myWorkspace" });
      }
      return page.organizationSummaries;
    }) || [];

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

  const handleEndReached = () => {
    if (hasNextPage) {
      fetchNextPage();
    }
  };

  useUpdateEffect(() => {
    closePopover();
  }, [closePopover, location.pathname, location.search]);

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  const estimatedHeight = !allowMultiWorkspace
    ? 64
    : Math.min(
        organizationSummaries.reduce((acc, curr) => {
          return acc + 50 + (curr?.workspaces?.length || 0) * 32;
        }, 0),
        388
      );

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
        {isLoading ? (
          <Box p="md" pb="sm">
            <LoadingSpinner />
          </Box>
        ) : organizationSummaries?.length === 0 ? (
          <Box p="md">
            <Text align="center">
              <FormattedMessage id="workspaces.noWorkspaces" />
            </Text>
          </Box>
        ) : (
          <Virtuoso
            ref={virtuosoRef}
            style={{ height: estimatedHeight }}
            data={organizationSummaries?.filter(isNonNullable)}
            endReached={handleEndReached}
            computeItemKey={(index, item) => item.organization.organizationId + index}
            itemContent={OrganizationWithWorkspaces}
            components={{
              Footer: isFetchingNextPage
                ? () => (
                    <Box pt="md" pb="sm">
                      <LoadingSpinner />
                    </Box>
                  )
                : undefined,
            }}
          />
        )}
      </FlexContainer>
    </>
  );
};
