import React, { useDeferredValue, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Message } from "components/ui/Message";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useListGroups } from "core/api";

import { GroupCard } from "./GroupCard";
import styles from "./GroupsList.module.scss";

const SEARCH_PARAM = "search";

export const GroupsList: React.FC = () => {
  const { data, isInitialLoading, isError } = useListGroups();
  const { formatMessage } = useIntl();

  const organizationInfo = useCurrentOrganizationInfo();
  const sso = organizationInfo?.sso;
  const scim = organizationInfo?.scim;

  const [searchParams, setSearchParams] = useSearchParams();
  const filterParam = searchParams.get(SEARCH_PARAM);
  const [groupFilter, setGroupFilter] = React.useState(filterParam ?? "");
  const deferredGroupFilter = useDeferredValue(groupFilter);

  // Replace-navigate so typing never stacks history entries. setSearchParams changes identity on
  // every URL change (it's a useCallback closing over the memoized searchParams), so this effect
  // re-runs after its own write; the guard below turns that re-run into a no-op instead of an
  // infinite replace loop, and also skips the redundant replace on mount when the URL already
  // matches.
  useEffect(() => {
    if ((searchParams.get(SEARCH_PARAM) ?? "") === deferredGroupFilter) {
      return;
    }

    setSearchParams(
      (params) => {
        const next = new URLSearchParams(params);
        if (deferredGroupFilter) {
          next.set(SEARCH_PARAM, deferredGroupFilter);
        } else {
          next.delete(SEARCH_PARAM);
        }
        return next;
      },
      { replace: true }
    );
  }, [deferredGroupFilter, searchParams, setSearchParams]);

  const groups = data?.groups ?? [];

  // The predicate reads the URL parameter rather than the local input state, so a shared or
  // reloaded URL filters identically. Both matched fields are visible on the card.
  const filterValue = filterParam?.toLowerCase() ?? "";
  const filteredGroups = groups.filter(
    (group) =>
      group.name.toLowerCase().includes(filterValue) ||
      (group.description?.toLowerCase().includes(filterValue) ?? false)
  );

  const isReady = !isInitialLoading && !isError;
  const hasNoGroups = isReady && groups.length === 0;
  const hasNoMatches = isReady && !hasNoGroups && filteredGroups.length === 0;
  const showList = isReady && !hasNoGroups && !hasNoMatches;

  const showScimBanner = scim || (isReady && groups.length > 0);

  return (
    <FlexContainer direction="column" gap="md">
      {showScimBanner && (
        <Message
          type="warning"
          text={formatMessage({
            id: scim ? "settings.organization.groups.scim.banner" : "settings.organization.groups.scim.enableBanner",
          })}
        />
      )}
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexItem className={styles.searchInputWrapper}>
          <SearchInput
            value={groupFilter}
            onChange={setGroupFilter}
            placeholder={formatMessage({ id: "settings.organization.groups.search.placeholder" })}
          />
        </FlexItem>
        <FlexContainer alignItems="center">
          {sso && (
            <Badge variant="blue" radius="2xs" uppercase={false}>
              <Text size="sm">
                <FormattedMessage id="settings.organization.groups.scim.ssoEnabled" />
              </Text>
            </Badge>
          )}
          {scim && (
            <Badge variant="teal" radius="2xs" uppercase={false}>
              <Text size="sm">
                <FormattedMessage id="settings.organization.groups.scim.scimEnabled" />
              </Text>
            </Badge>
          )}
        </FlexContainer>
      </FlexContainer>
      {isInitialLoading && (
        <Box py="xl">
          <LoadingSpinner />
        </Box>
      )}
      {isError && (
        <Box py="xl">
          <Text color="red">
            <FormattedMessage id="settings.organization.groups.list.error" />
          </Text>
        </Box>
      )}
      {hasNoGroups && (
        <Box py="xl">
          <EmptyState
            text={<FormattedMessage id="settings.organization.groups.empty" />}
            description={<FormattedMessage id="settings.organization.groups.empty.description" />}
          />
        </Box>
      )}
      {hasNoMatches && (
        <Box py="xl" pl="lg">
          <Text color="grey" italicized>
            <FormattedMessage id="settings.organization.groups.noSearchResults" />
          </Text>
        </Box>
      )}
      {showList && (
        <FlexContainer direction="column" gap="md">
          {filteredGroups.map((group) => (
            <GroupCard key={group.groupId} group={group} />
          ))}
        </FlexContainer>
      )}
    </FlexContainer>
  );
};
