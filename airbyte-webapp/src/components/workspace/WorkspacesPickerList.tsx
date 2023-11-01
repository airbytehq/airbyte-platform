import { HTMLAttributes, useEffect, useMemo, useRef, useState, forwardRef, Ref } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useUpdateEffect } from "react-use";
import { Virtuoso, ItemContent, ComputeItemKey, VirtuosoHandle } from "react-virtuoso";

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

const ListRow: ItemContent<CloudWorkspaceRead | WorkspaceRead, null> = (_index, workspace) => {
  return (
    <Link variant="primary" to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}`}>
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

const computeItemKey: ComputeItemKey<CloudWorkspaceRead | WorkspaceRead, null> = (_index, { workspaceId }) =>
  workspaceId;

// Virtuoso's `List` ref is an HTMLDivElement so we're coercing some types here
const UlList = forwardRef<HTMLDivElement>((props, ref) => (
  <ul
    ref={ref as Ref<HTMLUListElement>}
    {...(props as HTMLAttributes<HTMLUListElement>)}
    className={styles.workspacesPickerList__list}
  />
));
UlList.displayName = "UlList";

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

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);
  useEffect(() => {
    virtuosoRef.current?.scrollTo({ top: 0 });
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
        <Virtuoso
          ref={virtuosoRef}
          style={{
            height: Math.min(
              400 /* max height */,
              window.innerHeight - 225 /* otherwise take up full height, minus gap above the popover */
            ),
            width: "100%",
          }}
          data={filteredWorkspaces}
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
