import { useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useUpdateEffect } from "react-use";
import { ListChildComponentProps, VariableSizeList } from "react-window";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { CloudWorkspaceRead, CloudWorkspaceReadList } from "core/api/types/CloudApi";
import { WorkspaceRead, WorkspaceReadList } from "core/request/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import styles from "./WorkspacesPickerList.module.scss";

interface WorkspacePickerListProps {
  loading: boolean;
  workspaces?: CloudWorkspaceReadList | WorkspaceReadList;
  closePicker: () => void;
}

export const WorkspacesPickerList: React.FC<WorkspacePickerListProps> = ({ loading, closePicker, workspaces }) => {
  const [workspaceFilter, setWorkspaceFilter] = useState("");
  const location = useLocation();

  useUpdateEffect(() => {
    closePicker();
  }, [closePicker, location.pathname, location.search]);

  const filteredWorkspaces = useMemo(() => {
    const filterableWorkspaces = workspaces?.workspaces as Array<WorkspaceRead | CloudWorkspaceRead>;

    return (
      filterableWorkspaces.filter((workspace) => {
        return workspace.name?.toLowerCase().includes(workspaceFilter.toLowerCase());
      }) ?? []
    );
  }, [workspaceFilter, workspaces]);

  // track known row heights to tell react-window to update when they are known
  const [rowHeights, setRowHeights] = useState<Map<number, number>>(new Map());

  // for optimization, allow updating a row height without re-rendering every row before it
  const [listRowToReset, setListRowToReset] = useState<number | null>(null);
  useEffect(() => {
    if (listRowToReset !== null) {
      variableSizeList.current?.resetAfterIndex(listRowToReset);
      setListRowToReset(null);
    }
  }, [listRowToReset]);
  const resetListRow = useMemo(() => {
    return (index: number) => {
      // if there was is no previous row to reset (null), reset at index;
      // otherwise reset at the earlier of the previous row or the current row
      setListRowToReset((previous) => (previous == null ? index : Math.min(previous, index)));
    };
  }, [setListRowToReset]);

  const variableSizeList = useRef<VariableSizeList>(null);

  const ESTIMATED_HEIGHT = 37; // single-line workspaces are 36.59 pixels in Chrome
  const getItemSize = useMemo(() => {
    return (rowIndex: number): number => {
      // report the height of a row, or the estimated height if unknown
      return rowHeights.get(rowIndex) ?? ESTIMATED_HEIGHT;
    };
  }, [rowHeights]);

  interface ListRowExtraProps {
    resetListRow: typeof resetListRow;
  }

  const ListRow = useMemo(() => {
    const ListRow: React.FC<ListChildComponentProps<ListRowExtraProps>> = ({ index, style, data }) => {
      const { resetListRow } = data;

      // `itemRef` is placed on a link inside the wrapping li
      // as we put `style` from react-window on the li itself, which contains the item height
      // the link is allowed to overflow and gives the proper height of the item
      // which, if different from the estimate, causes a re-render to fix the li height
      const itemRef = useRef<HTMLAnchorElement>(null);

      // after this item renders, measure its height and store it in rowHeights
      useEffect(() => {
        if (itemRef.current) {
          const element = itemRef.current;
          setRowHeights((rowHeights) => {
            const nextRowHeights = new Map(rowHeights);
            nextRowHeights.set(index, element.getBoundingClientRect().height);
            return nextRowHeights;
          });
          resetListRow(index);
        }
      }, [index, resetListRow]);

      const workspace = filteredWorkspaces[index];

      return (
        <li key={workspace.workspaceId} style={style}>
          <Link ref={itemRef} variant="primary" to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}`}>
            <Box py="md" px="md" className={styles.workspacesPickerList__item}>
              <FlexContainer direction="column" justifyContent="center" gap="sm">
                <Text align="left" color="blue" bold size="md">
                  {workspace.name}
                </Text>
              </FlexContainer>
            </Box>
          </Link>
        </li>
      );
    };
    return ListRow;
  }, [filteredWorkspaces]);

  return loading ? (
    <Box p="lg">
      <LoadingSpinner />
    </Box>
  ) : (
    <div>
      <div className={styles.workspaceSearch}>
        <SearchInput value={workspaceFilter} onChange={(e) => setWorkspaceFilter(e.target.value)} inline />
      </div>
      {!filteredWorkspaces.length ? (
        <Box p="md">
          <FormattedMessage id="workspaces.noWorkspaces" />
        </Box>
      ) : (
        <ul className={styles.workspacesPickerList__list}>
          <VariableSizeList<ListRowExtraProps>
            ref={variableSizeList}
            innerElementType="ul"
            height={Math.min(
              400 /* max height */,
              window.innerHeight - 225 /* otherwise take up full height, minus gap above the popover */
            )}
            itemCount={filteredWorkspaces.length}
            estimatedItemSize={ESTIMATED_HEIGHT}
            itemSize={getItemSize}
            width="100%"
            itemData={{ resetListRow }}
          >
            {ListRow}
          </VariableSizeList>
        </ul>
      )}

      <Box py="lg">
        <Link variant="primary" to={`/${RoutePaths.Workspaces}`}>
          <Text color="blue" size="md" bold align="center">
            <FormattedMessage id="workspaces.seeAll" />
          </Text>
        </Link>
      </Box>
    </div>
  );
};
