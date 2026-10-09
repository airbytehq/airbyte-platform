import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Switch } from "components/ui/Switch";
import { Table, TableColumns } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { useCurrentOrganizationId } from "area/organization/utils";
import { getOrganizationUsagePath } from "cloud/views/settings/routePaths";
import {
  useAgentsProvisioningStatusQuery,
  useUnenrollOrganizationFromAgents,
  useEnrollOrganizationInAgents,
  useFusionWorkspaceConnectors,
  useListWorkspacesInOrganization,
  useSetFusionActorEnablement,
  useEnableFusionWorkspaceActors,
  FusionWorkspaceConnector,
} from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

import styles from "./ContextLayerPage.module.scss";
import { useConfirmContextLayerDisable } from "./useConfirmContextLayerDisable";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface WorkspaceConnectorData {
  workspaceId: string;
  workspaceName: string;
}

type ActorKind = "source" | "destination";
type AgentAccessConnector = FusionWorkspaceConnector & { kind: ActorKind };
interface WorkspaceConnectorInventory {
  count: number | null;
  loading: boolean;
  canManageSources: boolean;
  canManageDestinations: boolean;
}

const WorkspaceConnectorTable: React.FC<{
  workspace: WorkspaceConnectorData;
  enabledConnectors: Record<string, boolean>;
  onToggle: (key: string, enabled: boolean) => void;
  onClearOptimistic: (key: string) => void;
  onInventoryChanged: (workspaceId: string, inventory: WorkspaceConnectorInventory) => void;
}> = ({ workspace, enabledConnectors, onToggle, onClearOptimistic, onInventoryChanged }) => {
  const [pendingConnectors, setPendingConnectors] = useState<Record<string, boolean>>({});
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { sources, destinations, sourcesLoading, destinationsLoading, sourcesError, destinationsError } =
    useFusionWorkspaceConnectors(workspace.workspaceId, { hydrate: true });
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const canManageSources = useGeneratedIntent(Intent.CreateOrEditSource, { workspaceId: workspace.workspaceId });
  const canManageDestinations = useGeneratedIntent(Intent.CreateOrEditDestination, {
    workspaceId: workspace.workspaceId,
  });
  const confirmDisable = useConfirmContextLayerDisable();
  const connectors: AgentAccessConnector[] = [
    ...sources.map((connector) => ({ ...connector, kind: "source" as const })),
    ...destinations.map((connector) => ({ ...connector, kind: "destination" as const })),
  ];
  const isLoading = sourcesLoading || destinationsLoading;
  const hasError = sourcesError || destinationsError;
  useEffect(() => {
    onInventoryChanged(workspace.workspaceId, {
      count: isLoading || hasError ? null : sources.length + destinations.length,
      loading: isLoading,
      canManageSources,
      canManageDestinations,
    });
  }, [
    canManageSources,
    canManageDestinations,
    sources.length,
    destinations.length,
    hasError,
    isLoading,
    onInventoryChanged,
    workspace.workspaceId,
  ]);
  const columns: TableColumns<AgentAccessConnector> = [
    {
      header: formatMessage({ id: "cloud.contextLayer.agentAccess.nameColumn" }),
      accessorKey: "name",
      meta: { thClassName: styles.connectorColumn, tdClassName: styles.connectorColumn },
      cell: ({ row }) => (
        <FlexContainer alignItems="center" gap="sm">
          <ConnectorIcon icon={row.original.icon} />
          <Text as="span" size="sm">
            {row.original.name}
          </Text>
        </FlexContainer>
      ),
    },
    {
      header: formatMessage({ id: "cloud.contextLayer.agentAccess.typeColumn" }),
      accessorKey: "kind",
      meta: { thClassName: styles.typeColumn, tdClassName: styles.typeColumn },
      cell: ({ row }) => (
        <Text size="sm" color="grey">
          <FormattedMessage id={row.original.kind === "source" ? "connector.source" : "connector.destination"} />
        </Text>
      ),
    },
    {
      id: "agentAccess",
      meta: { thClassName: styles.controlColumn, tdClassName: styles.controlColumn },
      header: () => (
        <FlexContainer alignItems="center" gap="sm">
          <FormattedMessage id="cloud.contextLayer.setup.agentAccess.title" />
          <Tooltip
            control={
              <button
                type="button"
                className={styles.infoTooltipButton}
                aria-label={formatMessage({ id: "cloud.contextLayer.agentAccess.tooltip" })}
              >
                <Icon type="infoOutline" size="xs" />
              </button>
            }
          >
            <FormattedMessage id="cloud.contextLayer.agentAccess.tooltip" />
          </Tooltip>
        </FlexContainer>
      ),
      cell: ({ row: { original: connector } }) => {
        const key = `${workspace.workspaceId}:${connector.kind}:${connector.id}`;
        const canManageConnectors = connector.kind === "source" ? canManageSources : canManageDestinations;
        const checked = connector.supported && (enabledConnectors[key] ?? connector.enabled);
        return (
          <Switch
            size="sm"
            checked={checked}
            disabled={
              !canManageConnectors ||
              !connector.supported ||
              connector.loading ||
              connector.error ||
              pendingConnectors[key]
            }
            onChange={
              canManageConnectors
                ? async (event) => {
                    const enabled = event.target.checked;
                    if (!enabled && !(await confirmDisable(connector.name))) {
                      return;
                    }
                    setPendingConnectors((current) => ({ ...current, [key]: true }));
                    onToggle(key, enabled);
                    try {
                      const result = await setFusionActorEnablement({
                        actorId: connector.id,
                        workspaceId: workspace.workspaceId,
                        expectedState: connector.state,
                        actorKind: connector.kind,
                        enabled,
                      });
                      onToggle(key, result.enabled);
                      onClearOptimistic(key);
                    } catch {
                      onClearOptimistic(key);
                      registerNotification({
                        id: `context-layer-connector-toggle-error-${key}`,
                        text: formatMessage(
                          { id: "cloud.contextLayer.connectors.toggleError" },
                          { name: connector.name }
                        ),
                        type: "error",
                      });
                    } finally {
                      setPendingConnectors((current) => ({ ...current, [key]: false }));
                    }
                  }
                : undefined
            }
            aria-label={formatMessage({ id: "cloud.contextLayer.agentAccess.controlLabel" }, { name: connector.name })}
          />
        );
      },
    },
  ];

  return (
    <div className={styles.workspaceGroup} data-testid={`context-layer-workspace-${workspace.workspaceId}`}>
      {(hasError || connectors.length > 0) && (
        <Heading as="h3" size="sm">
          {workspace.workspaceName}
        </Heading>
      )}
      {sourcesError && (
        <Text color="grey" data-testid="context-layer-source-error">
          <FormattedMessage id="cloud.contextLayer.agentAccess.sourcesError" />
        </Text>
      )}
      {destinationsError && (
        <Text color="grey" data-testid="context-layer-destination-error">
          <FormattedMessage id="cloud.contextLayer.agentAccess.destinationsError" />
        </Text>
      )}
      {connectors.length > 0 && (
        <div className={styles.tableWrapper}>
          <Table
            className={styles.connectorTable}
            columns={columns}
            data={connectors}
            rowId={(connector) => `${connector.kind}:${connector.id}`}
            variant="light"
            sorting={false}
            stickyHeaders={false}
            showEmptyPlaceholder={false}
          />
        </div>
      )}
    </div>
  );
};

interface AddConnectorButtonProps {
  kind: ActorKind;
  workspaceId: string;
}

const AddConnectorButton: React.FC<AddConnectorButtonProps> = ({ kind, workspaceId }) => {
  const navigate = useNavigate();
  const setupRoute =
    kind === "source"
      ? `${RoutePaths.Source}/${SourcePaths.SelectSourceNew}`
      : `${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}`;
  const labelId =
    kind === "source" ? "cloud.contextLayer.sources.empty.add" : "cloud.contextLayer.destinations.empty.add";

  return (
    <Button
      type="button"
      variant="primary"
      className={styles.emptyStateButton}
      onClick={() => navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${setupRoute}`)}
    >
      <FormattedMessage id={labelId} />
    </Button>
  );
};

const WorkspaceConnectorAccess: React.FC<{
  workspacesQuery: ReturnType<typeof useListWorkspacesInOrganization>;
}> = ({ workspacesQuery }) => {
  const [enabledConnectors, setEnabledConnectors] = useState<Record<string, boolean>>({});
  const [connectorInventories, setConnectorInventories] = useState<Record<string, WorkspaceConnectorInventory>>({});
  const [visibleWorkspaceCount, setVisibleWorkspaceCount] = useState(25);
  const organizationId = useCurrentOrganizationId();
  const [organizationWorkspaceMap] = useLocalStorage("airbyte_organization-workspace-map", {});
  const { fetchNextPage, hasNextPage, isFetchingNextPage, isError } = workspacesQuery;
  const onInventoryChanged = useCallback((workspaceId: string, inventory: WorkspaceConnectorInventory) => {
    setConnectorInventories((current) => {
      const previous = current[workspaceId];
      return previous?.count === inventory.count &&
        previous?.loading === inventory.loading &&
        previous?.canManageSources === inventory.canManageSources &&
        previous?.canManageDestinations === inventory.canManageDestinations
        ? current
        : { ...current, [workspaceId]: inventory };
    });
  }, []);

  const allWorkspaces = useMemo<WorkspaceConnectorData[]>(() => {
    const apiWorkspaces = workspacesQuery.data?.pages.flatMap((page) => page.workspaces ?? []) ?? [];
    return apiWorkspaces.map((workspace) => ({
      workspaceId: workspace.workspaceId,
      workspaceName: workspace.name,
    }));
  }, [workspacesQuery.data?.pages]);
  const workspaces = allWorkspaces.slice(0, visibleWorkspaceCount);
  const hasMoreWorkspaces = allWorkspaces.length > visibleWorkspaceCount || hasNextPage;
  const loadMoreWorkspaces = async () => {
    if (allWorkspaces.length <= visibleWorkspaceCount && hasNextPage) {
      const nextPage = await fetchNextPage();
      if (nextPage.isError) {
        return;
      }
    }
    setVisibleWorkspaceCount((count) => count + 25);
  };
  const inventoriesLoaded =
    workspaces.length > 0 &&
    workspaces.every(
      (workspace) =>
        workspace.workspaceId in connectorInventories && !connectorInventories[workspace.workspaceId].loading
    );
  const isEmpty =
    inventoriesLoaded &&
    !hasMoreWorkspaces &&
    !isFetchingNextPage &&
    workspaces.every((workspace) => connectorInventories[workspace.workspaceId].count === 0);
  const storedWorkspaceId = organizationWorkspaceMap[organizationId];
  const targetWorkspaceId = (kind: ActorKind) => {
    const canManage = (workspace: WorkspaceConnectorData) =>
      kind === "source"
        ? connectorInventories[workspace.workspaceId]?.canManageSources
        : connectorInventories[workspace.workspaceId]?.canManageDestinations;
    return (
      workspaces.find((workspace) => workspace.workspaceId === storedWorkspaceId && canManage(workspace))
        ?.workspaceId ?? workspaces.find(canManage)?.workspaceId
    );
  };

  return (
    <div className={styles.workspaceSection}>
      <Heading as="h2" size="lg">
        <FormattedMessage id="cloud.contextLayer.agentAccess.pageTitle" />
      </Heading>
      <Text className={styles.workspaceDescription}>
        <FormattedMessage id="cloud.contextLayer.agentAccess.description.enabled" />
      </Text>
      {workspacesQuery.isLoading ? (
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.workspace.loading" />
        </Text>
      ) : workspaces.length === 0 ? (
        <Text color="grey">
          <FormattedMessage id="cloud.contextLayer.workspace.empty" />
        </Text>
      ) : (
        <>
          {isEmpty && (
            <EmptyState
              icon="aiStars"
              text={<FormattedMessage id="cloud.contextLayer.agentAccess.empty.title" />}
              description={<FormattedMessage id="cloud.contextLayer.agentAccess.empty.description" />}
              button={
                <FlexContainer gap="md">
                  {(["source", "destination"] as const).map((kind) => {
                    const workspaceId = targetWorkspaceId(kind);
                    return workspaceId && <AddConnectorButton key={kind} kind={kind} workspaceId={workspaceId} />;
                  })}
                </FlexContainer>
              }
            />
          )}
          {workspaces.map((workspace) => (
            <div
              key={workspace.workspaceId}
              hidden={
                isEmpty ||
                (workspace.workspaceId in connectorInventories &&
                  connectorInventories[workspace.workspaceId].count === 0)
              }
            >
              <WorkspaceConnectorTable
                workspace={workspace}
                enabledConnectors={enabledConnectors}
                onInventoryChanged={onInventoryChanged}
                onToggle={(key, enabled) => setEnabledConnectors((current) => ({ ...current, [key]: enabled }))}
                onClearOptimistic={(key) =>
                  setEnabledConnectors((current) => {
                    const next = { ...current };
                    delete next[key];
                    return next;
                  })
                }
              />
            </div>
          ))}
          {hasMoreWorkspaces && !isFetchingNextPage && !isError && (
            <Button type="button" variant="secondary" onClick={() => void loadMoreWorkspaces()}>
              <FormattedMessage id="cloud.contextLayer.workspace.loadMore" />
            </Button>
          )}
          {!inventoriesLoaded && !isFetchingNextPage && (
            <Text color="grey">
              <FormattedMessage id="cloud.contextLayer.workspace.connectorsLoading" />
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
      )}
    </div>
  );
};

const ContextLayerToggle: React.FC<{ enabled: boolean; disabled: boolean; onClick?: () => void }> = ({
  enabled,
  disabled,
  onClick,
}) => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.toggleRow}>
      <Heading as="h2" size="sm">
        <FormattedMessage id="cloud.contextLayer.organizationAgentAccess.title" />
      </Heading>
      <Switch
        size="sm"
        checked={enabled}
        disabled={disabled || !onClick}
        onChange={onClick}
        aria-label={formatMessage({ id: "cloud.contextLayer.organizationAgentAccess.title" })}
      />
    </div>
  );
};

const ContextLayerUnavailable: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="xl">
        <div className={styles.header}>
          <Heading as="h1" size="md">
            <FormattedMessage id="cloud.contextLayer.title" />
          </Heading>
          <Text className={styles.subtitle}>
            <FormattedMessage id="cloud.contextLayer.subtitle" />
          </Text>
        </div>
        <Card
          title={formatMessage({ id: "cloud.contextLayer.unavailable.title" })}
          dataTestId="context-layer-unavailable"
        >
          <div className={styles.cardContent}>
            <Text>
              <FormattedMessage id="cloud.contextLayer.unavailable.description" />
            </Text>
            <ExternalLink href={links.contextLayerDocs} opensInNewTab>
              <FormattedMessage id="cloud.contextLayer.docs" />
            </ExternalLink>
          </div>
        </Card>
      </FlexContainer>
    </div>
  );
};

const ContextLayerLoadError: React.FC<{ onRetry: () => void }> = ({ onRetry }) => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="xl">
        <div className={styles.header}>
          <Heading as="h1" size="md">
            <FormattedMessage id="cloud.contextLayer.title" />
          </Heading>
          <Text className={styles.subtitle}>
            <FormattedMessage id="cloud.contextLayer.subtitle" />
          </Text>
        </div>
        <Card title={formatMessage({ id: "cloud.contextLayer.loadError.title" })} dataTestId="context-layer-load-error">
          <div className={styles.cardContent}>
            <Text>
              <FormattedMessage id="cloud.contextLayer.loadError.description" />
            </Text>
            <FlexContainer>
              <Button variant="secondary" onClick={onRetry}>
                <FormattedMessage id="form.tryAgain" />
              </Button>
            </FlexContainer>
          </div>
        </Card>
      </FlexContainer>
    </div>
  );
};

const ContextLayerPageContent: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const statusQuery = useAgentsProvisioningStatusQuery({ enabled: isCloudApp });
  const status = statusQuery.data;
  const isEligible = Boolean(status && (status.is_enrolled || (status.external_cloud_eligible && showAgentsOptIn)));
  const enrollOrganization = useEnrollOrganizationInAgents();
  const enableWorkspaceActors = useEnableFusionWorkspaceActors();
  const unenrollOrganization = useUnenrollOrganizationFromAgents();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });
  const canViewOrganizationSettings = useGeneratedIntent(Intent.ViewOrganizationSettings, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: 25, rowOffset: 0 },
    enabled: isEligible,
  });
  const [isEnabling, setIsEnabling] = useState(false);

  if (isCloudApp && statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (isCloudApp && statusQuery.isError) {
    return <ContextLayerLoadError onRetry={() => statusQuery.refetch()} />;
  }
  if (!isCloudApp || !isEligible || !status) {
    return <ContextLayerUnavailable />;
  }

  const enableAgentAccess = async () => {
    let enrolled = false;
    setIsEnabling(true);
    try {
      let workspaceData = workspacesQuery.data;
      let hasNextPage = workspacesQuery.hasNextPage;

      if (!workspaceData) {
        const refreshed = await workspacesQuery.refetch();
        if (refreshed.isError) {
          throw refreshed.error;
        }
        workspaceData = refreshed.data;
        hasNextPage = (refreshed as { hasNextPage?: boolean }).hasNextPage ?? hasNextPage;
      }

      while (hasNextPage) {
        const nextPage = await workspacesQuery.fetchNextPage();
        if (nextPage.isError) {
          throw nextPage.error;
        }
        workspaceData = nextPage.data;
        hasNextPage = nextPage.hasNextPage;
      }

      const workspaceIds =
        workspaceData?.pages.flatMap((page) => page.workspaces ?? []).map((workspace) => workspace.workspaceId) ?? [];

      if (workspaceIds.length === 0) {
        throw new Error("No workspaces available for enrollment");
      }

      await enrollOrganization.mutateAsync({ workspaceIds, addAllSupportedActors: false });
      enrolled = true;
      const failedActors = await enableWorkspaceActors.mutateAsync({ workspaceIds });
      if (failedActors.length > 0) {
        throw new Error("Connector enablement failed");
      }
    } catch {
      registerNotification({
        id: "context-layer-enrollment-error",
        text: formatMessage({
          id: enrolled ? "cloud.contextLayer.enable.sourcesError" : "cloud.contextLayer.enable.error",
        }),
        type: "error",
      });
    } finally {
      setIsEnabling(false);
    }
  };

  const openDisableConfirmation = () => {
    openConfirmationModal({
      title: "cloud.contextLayer.disableOrg.title",
      text: "cloud.contextLayer.disableOrg.text",
      submitButtonText: "cloud.contextLayer.disableOrg.submit",
      submitButtonVariant: "danger",
      submitButtonDataId: "context-layer-disable-org-confirm",
      onSubmit: async () => {
        try {
          await unenrollOrganization.mutateAsync();
          closeConfirmationModal();
        } catch {
          registerNotification({
            id: "context-layer-disable-org-error",
            text: formatMessage({ id: "cloud.contextLayer.disableOrg.error" }),
            type: "error",
          });
        }
      },
    });
  };

  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="xl">
        <div className={styles.header}>
          <Heading as="h1" size="md">
            <FormattedMessage id="cloud.contextLayer.title" />
          </Heading>
          <Text className={styles.subtitle}>
            {status.is_enrolled ? (
              <>
                <FormattedMessage id="cloud.contextLayer.subtitle.enabled" />
                {canViewOrganizationSettings && canViewOrganizationUsage && (
                  <>
                    {" "}
                    <Link to={getOrganizationUsagePath(organizationId)}>
                      <FormattedMessage id="cloud.contextLayer.subtitle.viewUsage" />
                    </Link>
                  </>
                )}
              </>
            ) : (
              <FormattedMessage id="cloud.contextLayer.subtitle" />
            )}
          </Text>
        </div>
        <Card dataTestId="context-layer-card">
          <div className={styles.cardContent}>
            <ContextLayerToggle
              enabled={status.is_enrolled}
              disabled={isEnabling}
              onClick={
                canManageOrganizationPermissions
                  ? status.is_enrolled
                    ? openDisableConfirmation
                    : () => void enableAgentAccess()
                  : undefined
              }
            />
            <Text>
              <FormattedMessage id="cloud.contextLayer.organizationAgentAccess.description" />
            </Text>
            {!status.is_enrolled && !canManageOrganizationPermissions && (
              <div data-testid="context-layer-admin-required">
                <Text color="grey">
                  <FormattedMessage id="cloud.contextLayer.enable.adminRequired" />
                </Text>
                <Text color="grey" size="sm">
                  <FormattedMessage id="cloud.contextLayer.enable.adminRequired.description" />
                </Text>
              </div>
            )}
          </div>
        </Card>
      </FlexContainer>
    </div>
  );
};

const ContextLayerConnectorsPageContent: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const statusQuery = useAgentsProvisioningStatusQuery({ enabled: isCloudApp });
  const status = statusQuery.data;
  const isEligible = Boolean(status && (status.is_enrolled || (status.external_cloud_eligible && showAgentsOptIn)));
  const navigate = useNavigate();
  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: 25, rowOffset: 0 },
    enabled: Boolean(status?.is_enrolled),
  });

  if (isCloudApp && statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (isCloudApp && statusQuery.isError) {
    return <ContextLayerLoadError onRetry={() => statusQuery.refetch()} />;
  }
  if (!isCloudApp || !isEligible || !status) {
    return <ContextLayerUnavailable />;
  }

  if (!status.is_enrolled) {
    return (
      <div className={styles.page}>
        <div className={styles.workspaceSection}>
          <Heading as="h2" size="lg">
            <FormattedMessage id="cloud.contextLayer.agentAccess.pageTitle" />
          </Heading>
          <Text className={styles.workspaceDescription}>
            <FormattedMessage id="cloud.contextLayer.agentAccess.description.disabled" />
          </Text>
          <EmptyState
            icon="chart"
            text={<FormattedMessage id="cloud.contextLayer.agentAccess.noAccess.title" />}
            description={<FormattedMessage id="cloud.contextLayer.agentAccess.noAccess.description" />}
            button={
              <Button
                type="button"
                variant="primary"
                className={styles.emptyStateButton}
                onClick={() => navigate(`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`)}
              >
                <FormattedMessage id="cloud.contextLayer.noAccess.settings" />
              </Button>
            }
          />
        </div>
      </div>
    );
  }

  return (
    <div className={styles.page}>
      <WorkspaceConnectorAccess key={organizationId} workspacesQuery={workspacesQuery} />
    </div>
  );
};

export const ContextLayerPage: React.FC = () => {
  return (
    <React.Suspense fallback={<LoadingPage />}>
      <ContextLayerPageContent />
    </React.Suspense>
  );
};

export const ContextLayerConnectorsPage: React.FC = () => {
  return (
    <React.Suspense fallback={<LoadingPage />}>
      <ContextLayerConnectorsPageContent />
    </React.Suspense>
  );
};
