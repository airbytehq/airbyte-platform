import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import DatePicker from "components/ui/DatePicker";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { LoadingPage } from "components/ui/LoadingPage";
import { Switch } from "components/ui/Switch";
import { Table, TableColumns } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useConnectionSemanticSearchEnablement,
  useOrganizationSemanticSearchEnablement,
  useSemanticSearchConnections,
  useUpdateConnectionSemanticSearchEnablement,
} from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useNotificationService } from "core/services/Notification";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import {
  ColumnHeader,
  ContextLayerLoadError,
  ContextLayerSettingsEmptyState,
  ContextLayerUnavailable,
} from "./ContextLayerPage";
import styles from "./SemanticSearchPage.module.scss";
import { useContextLayerEligibility } from "./useContextLayerEligibility";
import { ContextLayerWorkspace, useContextLayerWorkspaces } from "./useContextLayerWorkspaces";

interface WorkspaceConnectionInventory {
  loading: boolean;
  // True only after a successful load that returned no eligible connections.
  empty: boolean;
  canCreateOrEditConnections: boolean;
}

const SemanticSearchSwitch: React.FC<{
  connection: WebBackendConnectionListItem;
  canCreateOrEditConnections: boolean;
}> = ({ connection, canCreateOrEditConnections }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const enablementQuery = useConnectionSemanticSearchEnablement(connection.connectionId);
  const updateEnablement = useUpdateConnectionSemanticSearchEnablement(connection.connectionId);
  const enabled = enablementQuery.data?.enable_indexing ?? false;

  const saveEnablement = async (nextEnabled: boolean) => {
    try {
      await updateEnablement.mutateAsync({ enabled: nextEnabled, expectedEnabled: enabled });
    } catch {
      registerNotification({
        id: "semantic-search-connection-toggle-error",
        text: formatMessage({ id: "cloud.contextLayer.semanticSearch.updateError" }),
        type: "error",
      });
    }
  };

  const openDisableConfirmation = () => {
    openConfirmationModal({
      title: "cloud.contextLayer.semanticSearch.disableConfirm.title",
      text: "cloud.contextLayer.semanticSearch.disableConfirm.text",
      submitButtonText: "cloud.contextLayer.semanticSearch.disableConfirm.submit",
      submitButtonVariant: "danger",
      submitButtonDataId: "semantic-search-connection-disable-confirm",
      onSubmit: async () => {
        await saveEnablement(false);
        // Close the dialog even when the save fails. The update hook reads the saved value again, so the dialog
        // may no longer match it.
        closeConfirmationModal();
      },
    });
  };

  if (enablementQuery.isError) {
    return (
      <Button type="button" size="xs" variant="secondary" onClick={() => enablementQuery.refetch()}>
        <FormattedMessage id="form.tryAgain" />
      </Button>
    );
  }

  return (
    <Switch
      size="sm"
      checked={enabled}
      disabled={!canCreateOrEditConnections || enablementQuery.isLoading || updateEnablement.isLoading}
      onChange={enabled ? openDisableConfirmation : () => void saveEnablement(true)}
      aria-label={formatMessage(
        { id: "cloud.contextLayer.semanticSearchPage.semanticSearchControlLabel" },
        { name: connection.name }
      )}
    />
  );
};

// The backfill controls stay disabled until the connection backfill endpoint exists.
const SemanticSearchConnectionTable: React.FC<{
  connections: WebBackendConnectionListItem[];
  canCreateOrEditConnections: boolean;
}> = ({ connections, canCreateOrEditConnections }) => {
  const { formatMessage } = useIntl();
  // Memoized because the table renders each cell function as a component: new functions would remount every
  // switch on each render and drop its pending update.
  const columns = useMemo<TableColumns<WebBackendConnectionListItem>>(
    () => [
      {
        id: "connection",
        header: formatMessage({ id: "cloud.contextLayer.semanticSearchPage.connectionColumn" }),
        meta: { responsive: true },
        cell: ({ row: { original: connection } }) => (
          <FlexContainer alignItems="center" gap="sm" className={styles.connectionCell}>
            <ConnectorIcon icon={connection.source.icon} />
            <Text as="span" size="sm" className={styles.connectorName}>
              {connection.source.name}
            </Text>
            <Icon type="arrowRight" size="xs" color="disabled" />
            <ConnectorIcon icon={connection.destination.icon} />
            <Text as="span" size="sm" className={styles.connectorName}>
              {connection.destination.name}
            </Text>
          </FlexContainer>
        ),
      },
      {
        id: "semanticSearch",
        header: () => (
          <ColumnHeader
            titleId="cloud.contextLayer.semanticSearchPage.semanticSearchColumn"
            tooltipId="cloud.contextLayer.semanticSearchPage.semanticSearchColumn.tooltip"
          />
        ),
        meta: { thClassName: styles.controlColumn, tdClassName: styles.controlColumn },
        cell: ({ row: { original: connection } }) => (
          <SemanticSearchSwitch connection={connection} canCreateOrEditConnections={canCreateOrEditConnections} />
        ),
      },
      {
        id: "backfillDate",
        header: () => (
          <ColumnHeader
            titleId="cloud.contextLayer.semanticSearchPage.backfillDateColumn"
            tooltipId="cloud.contextLayer.semanticSearchPage.backfillDateColumn.tooltip"
          />
        ),
        meta: { thClassName: styles.backfillDateColumn, tdClassName: styles.backfillDateColumn },
        cell: ({ row: { original: connection } }) => {
          // DatePicker does not accept an aria-label, so a hidden label names its input.
          const inputId = `semantic-search-backfill-date-${connection.connectionId}`;
          return (
            <>
              <label htmlFor={inputId} className={styles.visuallyHidden}>
                {formatMessage(
                  { id: "cloud.contextLayer.semanticSearchPage.backfillDateControlLabel" },
                  { name: connection.name }
                )}
              </label>
              <DatePicker id={inputId} value="" onChange={() => undefined} disabled withTime />
            </>
          );
        },
      },
      {
        id: "backfill",
        header: () => (
          <ColumnHeader
            titleId="cloud.contextLayer.semanticSearchPage.backfillColumn"
            tooltipId="cloud.contextLayer.semanticSearchPage.backfillColumn.tooltip"
          />
        ),
        meta: { thClassName: styles.controlColumn, tdClassName: styles.controlColumn },
        cell: ({ row: { original: connection } }) => (
          <Button
            type="button"
            variant="secondary"
            disabled
            aria-label={formatMessage(
              { id: "cloud.contextLayer.semanticSearchPage.startBackfillControlLabel" },
              { name: connection.name }
            )}
          >
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.startBackfill" />
          </Button>
        ),
      },
      {
        id: "backfillStatus",
        header: () => (
          <ColumnHeader
            titleId="cloud.contextLayer.semanticSearchPage.backfillStatusColumn"
            tooltipId="cloud.contextLayer.semanticSearchPage.backfillStatusColumn.tooltip"
          />
        ),
        meta: { thClassName: styles.controlColumn, tdClassName: styles.controlColumn },
        cell: () => (
          <Badge variant="grey" uppercase={false} className={styles.statusBadge}>
            <Icon type="minusCircle" size="xs" />
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillOff" />
          </Badge>
        ),
      },
    ],
    [canCreateOrEditConnections, formatMessage]
  );

  return (
    <div className={styles.tableWrapper}>
      <Table
        className={styles.connectionTable}
        columns={columns}
        data={connections}
        rowId="connectionId"
        variant="light"
        sorting={false}
        stickyHeaders={false}
        showEmptyPlaceholder={false}
      />
    </div>
  );
};

const SemanticSearchWorkspaceConnections: React.FC<{
  workspace: ContextLayerWorkspace;
  onInventoryChanged: (workspaceId: string, inventory: WorkspaceConnectionInventory) => void;
}> = ({ workspace, onInventoryChanged }) => {
  const { data, isLoading, isError, refetch, hasNextPage, fetchNextPage, isFetchingNextPage } =
    useSemanticSearchConnections(workspace.workspaceId);
  const canCreateOrEditConnections = useGeneratedIntent(Intent.CreateOrEditConnection, {
    workspaceId: workspace.workspaceId,
  });
  const connections = useMemo(() => data?.pages.flatMap((page) => page.connections) ?? [], [data?.pages]);
  const empty = !isLoading && !isError && connections.length === 0;

  useEffect(() => {
    onInventoryChanged(workspace.workspaceId, { loading: isLoading, empty, canCreateOrEditConnections });
  }, [canCreateOrEditConnections, empty, isLoading, onInventoryChanged, workspace.workspaceId]);

  // Loaded rows with a next page retry through "Load more connections".
  const canRetryWithLoadMore = connections.length > 0 && hasNextPage;

  return (
    <FlexContainer direction="column" gap="lg" data-testid={`semantic-search-workspace-${workspace.workspaceId}`}>
      {(isError || connections.length > 0) && (
        <Heading as="h3" size="sm">
          {workspace.workspaceName}
        </Heading>
      )}
      {isError && (
        <FlexContainer alignItems="center">
          <Text color="grey">
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.connectionsError" />
          </Text>
          {!canRetryWithLoadMore && (
            <Button type="button" variant="secondary" onClick={() => refetch()}>
              <FormattedMessage id="form.tryAgain" />
            </Button>
          )}
        </FlexContainer>
      )}
      {connections.length > 0 && (
        <SemanticSearchConnectionTable
          connections={connections}
          canCreateOrEditConnections={canCreateOrEditConnections}
        />
      )}
      {hasNextPage && (
        <FlexContainer>
          <Button type="button" variant="secondary" isLoading={isFetchingNextPage} onClick={() => void fetchNextPage()}>
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.loadMoreConnections" />
          </Button>
        </FlexContainer>
      )}
    </FlexContainer>
  );
};

const AddConnectionButton: React.FC<{ workspaceId: string }> = ({ workspaceId }) => {
  const navigate = useNavigate();

  return (
    <Button
      type="button"
      variant="primary"
      className={styles.emptyStateButton}
      onClick={() =>
        navigate(
          `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`
        )
      }
    >
      <FormattedMessage id="cloud.contextLayer.semanticSearchPage.empty.add" />
    </Button>
  );
};

const SemanticSearchWorkspaces: React.FC = () => {
  const [connectionInventories, setConnectionInventories] = useState<Record<string, WorkspaceConnectionInventory>>({});
  const organizationId = useCurrentOrganizationId();
  const [organizationWorkspaceMap] = useLocalStorage("airbyte_organization-workspace-map", {});
  const { workspacesQuery, workspaces, hasMoreWorkspaces, loadMoreWorkspaces } = useContextLayerWorkspaces();
  const { hasNextPage, isFetchingNextPage, isError } = workspacesQuery;
  const onInventoryChanged = useCallback((workspaceId: string, inventory: WorkspaceConnectionInventory) => {
    setConnectionInventories((current) => {
      const previous = current[workspaceId];
      return previous?.empty === inventory.empty &&
        previous?.loading === inventory.loading &&
        previous?.canCreateOrEditConnections === inventory.canCreateOrEditConnections
        ? current
        : { ...current, [workspaceId]: inventory };
    });
  }, []);

  if (workspacesQuery.isLoading) {
    return (
      <Text color="grey">
        <FormattedMessage id="cloud.contextLayer.workspace.loading" />
      </Text>
    );
  }
  if (isError && workspaces.length === 0) {
    return (
      <FlexContainer alignItems="center">
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.workspace.loadError" />
        </Text>
        <Button type="button" variant="secondary" onClick={() => workspacesQuery.refetch()}>
          <FormattedMessage id="form.tryAgain" />
        </Button>
      </FlexContainer>
    );
  }
  if (workspaces.length === 0) {
    return (
      <Text color="grey">
        <FormattedMessage id="cloud.contextLayer.workspace.empty" />
      </Text>
    );
  }

  const inventoriesLoaded = workspaces.every(
    (workspace) => connectionInventories[workspace.workspaceId]?.loading === false
  );
  const loadedWorkspacesEmpty = workspaces.every((workspace) => connectionInventories[workspace.workspaceId]?.empty);
  const isEmpty = loadedWorkspacesEmpty && !hasMoreWorkspaces && !isFetchingNextPage;

  // Prefer the last-visited workspace, then the first workspace where the viewer can create a connection.
  const storedWorkspaceId = organizationWorkspaceMap[organizationId];
  const canCreateOrEditConnections = (workspace: ContextLayerWorkspace) =>
    connectionInventories[workspace.workspaceId]?.canCreateOrEditConnections;
  const targetWorkspaceId =
    workspaces.find((workspace) => workspace.workspaceId === storedWorkspaceId && canCreateOrEditConnections(workspace))
      ?.workspaceId ?? workspaces.find(canCreateOrEditConnections)?.workspaceId;

  return (
    <>
      {isEmpty && (
        <EmptyState
          icon="chart"
          text={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.empty.title" />}
          description={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.empty.description" />}
          button={targetWorkspaceId && <AddConnectionButton workspaceId={targetWorkspaceId} />}
        />
      )}
      {workspaces.map((workspace) => (
        // Keep hidden workspaces mounted: each one fetches its connections and reports its count.
        <div key={workspace.workspaceId} hidden={connectionInventories[workspace.workspaceId]?.empty}>
          <SemanticSearchWorkspaceConnections workspace={workspace} onInventoryChanged={onInventoryChanged} />
        </div>
      ))}
      {loadedWorkspacesEmpty && !isEmpty && (
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.emptyLoaded" />
        </Text>
      )}
      {hasMoreWorkspaces && !isFetchingNextPage && !isError && (
        <FlexContainer>
          <Button type="button" variant="secondary" onClick={() => void loadMoreWorkspaces()}>
            <FormattedMessage id="cloud.contextLayer.workspace.loadMore" />
          </Button>
        </FlexContainer>
      )}
      {!inventoriesLoaded && !isFetchingNextPage && (
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.connectionsLoading" />
        </Text>
      )}
      {isFetchingNextPage && (
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.workspace.loadingMore" />
        </Text>
      )}
      {isError && hasNextPage && !isFetchingNextPage && (
        <FlexContainer alignItems="center">
          <Text color="grey">
            <FormattedMessage id="cloud.contextLayer.workspace.loadMoreError" />
          </Text>
          <Button type="button" variant="secondary" onClick={() => void loadMoreWorkspaces()}>
            <FormattedMessage id="form.tryAgain" />
          </Button>
        </FlexContainer>
      )}
    </>
  );
};

const SemanticSearchSettingSection: React.FC = () => {
  const semanticSearchQuery = useOrganizationSemanticSearchEnablement();
  const organizationId = useCurrentOrganizationId();

  if (semanticSearchQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (semanticSearchQuery.isError) {
    return <ContextLayerLoadError onRetry={() => semanticSearchQuery.refetch()} />;
  }
  if (!semanticSearchQuery.data) {
    return <LoadingPage />;
  }

  const semanticSearchEnabled = semanticSearchQuery.data.semanticSearchEnabled;
  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer direction="column" gap="md">
          <Heading as="h2" size="lg">
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.title" />
          </Heading>
          <Text>
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.description" />
          </Text>
        </FlexContainer>
        {semanticSearchEnabled ? (
          // Reset visible workspaces and inventories when the organization changes.
          <SemanticSearchWorkspaces key={organizationId} />
        ) : (
          <ContextLayerSettingsEmptyState
            title={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.disabled.title" />}
            description={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.disabled.description" />}
          />
        )}
      </FlexContainer>
    </div>
  );
};

const SemanticSearchPageContent: React.FC = () => {
  const { isCloudApp, statusQuery, status, isEligible } = useContextLayerEligibility();

  if (isCloudApp && statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (isCloudApp && statusQuery.isError) {
    return <ContextLayerLoadError onRetry={() => statusQuery.refetch()} />;
  }
  if (!isCloudApp || !isEligible || !status) {
    return <ContextLayerUnavailable />;
  }

  return <SemanticSearchSettingSection />;
};

export const SemanticSearchPage: React.FC = () => {
  return (
    <React.Suspense fallback={<LoadingPage />}>
      <SemanticSearchPageContent />
    </React.Suspense>
  );
};
