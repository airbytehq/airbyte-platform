import { Popover, PopoverButton, PopoverPanel, useClose } from "@headlessui/react";
import classNames from "classnames";
import { useRef, useState } from "react";
import { flushSync } from "react-dom";
import { FormattedMessage, useIntl } from "react-intl";
// eslint-disable-next-line no-restricted-imports
import { Link } from "react-router-dom";
import { Components, Virtuoso, VirtuosoHandle, ItemContent } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useListWorkspacesInOrganization } from "core/api";
import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import styles from "./WorkspacesPickerNext.module.scss";

interface WorkspacesPickerNextProps {
  currentWorkspace: WorkspaceRead;
}

export const WorkspacesPickerNext: React.FC<WorkspacesPickerNextProps> = ({ currentWorkspace }) => {
  return (
    <Popover className={styles.workspacesPicker}>
      <PopoverButton className={styles.workspacesPicker__button}>
        <Text as="span" className={styles.workspacesPicker__orgName}>
          {currentWorkspace?.name}
        </Text>
        <Icon type="chevronUpDown" size="xs" color="disabled" className={styles.workspacesPicker__icon} />
      </PopoverButton>
      <PopoverPanel anchor={{ to: "bottom start" }} className={styles.workspacesPicker__panel}>
        <WorkspacePickerPanelContent currentWorkspace={currentWorkspace} />
      </PopoverPanel>
    </Popover>
  );
};

interface WorkspacesPickerNextProps {
  currentWorkspace: WorkspaceRead;
}

interface WorkspacePickerContext {
  isFetchingNextPage: boolean;
  closePopover: () => void;
  currentWorkspaceId: string;
}

const WorkspacePickerPanelContent: React.FC<WorkspacesPickerNextProps> = ({ currentWorkspace }) => {
  const organizationId = useCurrentOrganizationId();
  const { formatMessage } = useIntl();
  const [searchValue, setSearchValue] = useState("");
  const {
    data: workspaces,
    isLoading,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useListWorkspacesInOrganization({
    organizationId,
    nameContains: searchValue,
    pagination: { pageSize: 10 },
  });
  const closePopover = useClose();

  const infiniteWorkspaces = workspaces?.pages.flatMap((page) => page.workspaces) ?? [];

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  return (
    <>
      <Box p="md">
        <SearchInput
          value={searchValue}
          onChange={(value) => {
            setSearchValue(value);
            virtuosoRef.current?.scrollTo({ top: 0 });
          }}
          placeholder={formatMessage({ id: "sidebar.searchAllWorkspaces" })}
          debounceTimeout={150}
        />
      </Box>
      {!isLoading && infiniteWorkspaces.length === 0 && (
        <Box px="md" pb="md">
          <Text color="grey" align="center">
            <FormattedMessage id="workspaces.noWorkspaces" />
          </Text>
        </Box>
      )}
      {isLoading && (
        <Box px="md" pb="md">
          <FlexContainer direction="column">
            <LoadingSkeleton />
            <LoadingSkeleton />
            <LoadingSkeleton />
          </FlexContainer>
        </Box>
      )}
      {!isLoading && infiniteWorkspaces.length > 0 && (
        <div className={styles.workspacesPicker__options}>
          <Virtuoso<WorkspaceRead, WorkspacePickerContext>
            ref={virtuosoRef}
            style={{
              height: 300,
              width: "100%",
            }}
            data={infiniteWorkspaces}
            context={{
              isFetchingNextPage,
              closePopover,
              currentWorkspaceId: currentWorkspace.workspaceId,
            }}
            endReached={() => {
              if (hasNextPage && !isFetchingNextPage) {
                fetchNextPage();
              }
            }}
            components={{
              Footer,
            }}
            itemContent={WorkspaceLink}
          />
        </div>
      )}
    </>
  );
};

const WorkspaceLink: ItemContent<WorkspaceRead, WorkspacePickerContext> = (_index, workspace, context) => {
  return (
    <Link
      key={workspace.workspaceId}
      onClick={() => flushSync(() => context.closePopover())}
      to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}`}
      className={classNames(styles.workspacesPicker__option, {
        [styles["workspacesPicker__option--selected"]]: workspace.workspaceId === context.currentWorkspaceId,
      })}
    >
      <Text bold={workspace.workspaceId === context.currentWorkspaceId}>{workspace.name}</Text>
    </Link>
  );
};

const Footer: Components<WorkspaceRead, WorkspacePickerContext>["Footer"] = ({ context }) => {
  if (!context?.isFetchingNextPage) {
    return null;
  }
  return (
    <Box px="md" pb="md">
      <FlexContainer direction="column" gap="md">
        <LoadingSkeleton />
        <LoadingSkeleton />
        <LoadingSkeleton />
      </FlexContainer>
    </Box>
  );
};
