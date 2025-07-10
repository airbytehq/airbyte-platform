import { HTMLAttributes, useRef, forwardRef, Ref, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce, useLocation, useUpdateEffect } from "react-use";
import { Virtuoso, ItemContent, ComputeItemKey, VirtuosoHandle } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useListWorkspacesInfinite } from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";
import { WORKSPACE_LIST_LENGTH } from "pages/workspaces/WorkspacesPage";

import styles from "./WorkspacesPickerList.module.scss";

interface WorkspacePickerListProps {
  closePicker: () => void;
}

const ListRow: ItemContent<WorkspaceRead, null> = (_index, workspace) => {
  return (
    <Link
      variant="primary"
      to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}`}
      data-testid={`workspace-picker-item-${workspace.workspaceId}`}
    >
      <Box py="md" px="md" className={styles.workspacesPickerList__item}>
        <FlexContainer direction="column" justifyContent="center" gap="sm">
          <Text align="left" color="blue" bold size="md">
            {workspace.name}
          </Text>
        </FlexContainer>
      </Box>
    </Link>
  );
};

const computeItemKey: ComputeItemKey<WorkspaceRead, null> = (_index, { workspaceId }) => workspaceId;

// Virtuoso's `List` ref is an HTMLDivElement so we're coercing some types here
const UlList = forwardRef<HTMLDivElement>((props, ref) => (
  <ul
    ref={ref as Ref<HTMLUListElement>}
    {...(props as HTMLAttributes<HTMLUListElement>)}
    className={styles.workspacesPickerList__list}
  />
));
UlList.displayName = "UlList";

export const WorkspacesPickerList: React.FC<WorkspacePickerListProps> = ({ closePicker }) => {
  const location = useLocation();

  const [searchValue, setSearchValue] = useState("");
  const [debouncedSearchValue, setDebouncedSearchValue] = useState("");

  const {
    data: workspacesData,
    hasNextPage,
    fetchNextPage,
    isLoading,
    isFetchingNextPage,
  } = useListWorkspacesInfinite(WORKSPACE_LIST_LENGTH, debouncedSearchValue);

  const workspaces = workspacesData?.pages.flatMap<WorkspaceRead>((page) => page.data.workspaces) ?? [];

  const handleEndReached = () => {
    if (hasNextPage) {
      fetchNextPage();
    }
  };

  useDebounce(
    () => {
      setDebouncedSearchValue(searchValue);
      virtuosoRef.current?.scrollTo({ top: 0 });
    },
    250,
    [searchValue]
  );

  useUpdateEffect(() => {
    closePicker();
  }, [closePicker, location.pathname, location.search]);

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  return (
    <>
      <SearchInput value={searchValue} onChange={setSearchValue} inline data-testid="workspaces-picker-input" />

      {isLoading ? (
        <Box p="lg">
          <LoadingSpinner />
        </Box>
      ) : (
        <div>
          {!workspaces || !workspaces.length ? (
            <Box p="md">
              <Text align="center">
                <FormattedMessage id="workspaces.noWorkspaces" />
              </Text>
            </Box>
          ) : (
            <Virtuoso
              ref={virtuosoRef}
              style={{
                height: Math.min(
                  400 /* max height */,
                  window.innerHeight - 225 /* otherwise take up full height, minus gap above the popover */
                ),
                width: "100%",
              }}
              data={workspaces}
              endReached={handleEndReached}
              increaseViewportBy={5}
              defaultItemHeight={37 /* single-line workspaces are 36.59 pixels in Chrome */}
              computeItemKey={computeItemKey}
              components={{
                List: UlList,

                // components are overly constrained to be a function/class component
                // but the string representation is fine; react-virtuoso defaults Item to `"div"`
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                Item: "li" as any,
              }}
              itemContent={ListRow}
            />
          )}
          {isFetchingNextPage && (
            <Box pt="sm">
              <LoadingSpinner />
            </Box>
          )}
          <Box py="lg">
            <Link variant="primary" to={`/${RoutePaths.Workspaces}`} data-testid="workspaces-picker-see-all">
              <Text color="blue" size="md" bold align="center">
                <FormattedMessage id="workspaces.seeAll" />
              </Text>
            </Link>
          </Box>
        </div>
      )}
    </>
  );
};
