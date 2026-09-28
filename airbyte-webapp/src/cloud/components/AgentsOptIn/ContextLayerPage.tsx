import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useAgentsProvisioningStatusQuery,
  useUnenrollOrganizationFromAgents,
  useEnrollOrganizationInAgents,
  useFusionWorkspaceConnectors,
  useListWorkspacesInOrganization,
  useSetFusionActorEnablement,
  useEnableFusionWorkspaceActors,
  FusionEnablementState,
} from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

import styles from "./ContextLayerPage.module.scss";
import { ContextLayerTosModal } from "./ContextLayerTosModal";
import { useConfirmContextLayerDisable } from "./useConfirmContextLayerDisable";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface Connector {
  id: string;
  name: string;
  enabled: boolean;
  supported: boolean;
  state?: FusionEnablementState;
  loading?: boolean;
  error?: boolean;
}

interface WorkspaceConnectorData {
  workspaceId: string;
  workspaceName: string;
}

type ActorKind = "source" | "destination";

const WorkspaceConnectorCard: React.FC<{
  workspace: WorkspaceConnectorData;
  enabledConnectors: Record<string, boolean>;
  onToggle: (key: string, enabled: boolean) => void;
  onClearOptimistic: (key: string) => void;
  actorKind: ActorKind;
  onInventoryLoaded: (workspaceId: string, count: number | null, canManageConnectors: boolean) => void;
}> = ({ workspace, enabledConnectors, onToggle, onClearOptimistic, actorKind, onInventoryLoaded }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [pendingConnectors, setPendingConnectors] = useState<Record<string, boolean>>({});
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { sources, destinations, sourcesLoading, destinationsLoading, sourcesError, destinationsError } =
    useFusionWorkspaceConnectors(workspace.workspaceId, { hydrate: isExpanded });
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const canManageConnectors = useGeneratedIntent(
    actorKind === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination,
    { workspaceId: workspace.workspaceId }
  );
  const confirmDisable = useConfirmContextLayerDisable();
  const connectors = actorKind === "source" ? sources : destinations;
  const isLoading = actorKind === "source" ? sourcesLoading : destinationsLoading;
  const hasError = actorKind === "source" ? sourcesError : destinationsError;
  useEffect(() => {
    if (!isLoading) {
      onInventoryLoaded(workspace.workspaceId, hasError ? null : connectors.length, canManageConnectors);
    }
  }, [canManageConnectors, connectors.length, hasError, isLoading, onInventoryLoaded, workspace.workspaceId]);
  const enabledCount = connectors.filter(
    (connector) =>
      connector.supported && (enabledConnectors[`${workspace.workspaceId}:${connector.id}`] ?? connector.enabled)
  ).length;
  const connectorRows = (connectors: Connector[], actorKind: "source" | "destination") =>
    connectors.map((connector) => {
      const key = `${workspace.workspaceId}:${connector.id}`;
      const checked = connector.supported && (enabledConnectors[key] ?? connector.enabled);
      return (
        <div className={styles.connectorRow} key={connector.id}>
          <div className={styles.connectorName}>
            <Icon type="file" size="sm" />
            <Text as="span" size="sm">
              {connector.name}
            </Text>
          </div>
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
                        actorKind,
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
            aria-label={connector.name}
          />
        </div>
      );
    });

  const renderConnectorSection = (
    labelId: string,
    connectors: Connector[],
    actorKind: "source" | "destination",
    count: number,
    hasError: boolean
  ) => {
    const supportedCount = connectors.filter((connector) => connector.supported).length;

    return (
      <div className={styles.connectorSection}>
        <div className={styles.connectorSectionHeader}>
          <Text size="xs" bold smallcaps>
            <FormattedMessage id={labelId} />
          </Text>
          {!hasError && (
            <span className={styles.countBadge}>
              <FormattedMessage
                id="cloud.contextLayer.workspace.enabledCount"
                values={{
                  enabled: count,
                  supported: supportedCount,
                  unsupported: connectors.length - supportedCount,
                }}
              />
            </span>
          )}
        </div>
        {hasError ? (
          <Text color="grey" data-testid={`context-layer-${actorKind}-error`}>
            <FormattedMessage id="cloud.contextLayer.connectors.error" />
          </Text>
        ) : (
          <div className={styles.connectorRows}>{connectorRows(connectors, actorKind)}</div>
        )}
      </div>
    );
  };

  const workspaceBodyId = `context-layer-workspace-body-${workspace.workspaceId}`;

  return (
    <div className={styles.workspaceCard} data-testid={`context-layer-workspace-${workspace.workspaceId}`}>
      <button
        type="button"
        className={styles.workspaceHeader}
        onClick={() => setIsExpanded((expanded) => !expanded)}
        aria-expanded={isExpanded}
        aria-controls={workspaceBodyId}
      >
        <div className={styles.workspaceHeaderInfo}>
          <Icon type={isExpanded ? "chevronDown" : "chevronRight"} size="sm" color="affordance" />
          <Text as="span" size="sm" bold>
            {workspace.workspaceName}
          </Text>
        </div>
        <div className={styles.workspaceCounts}>
          <Text as="span" size="xs" color="grey">
            <FormattedMessage
              id="cloud.contextLayer.workspace.kindCount"
              values={{ count: connectors.length, actorKind }}
            />
          </Text>
        </div>
      </button>
      {isExpanded && (
        <div id={workspaceBodyId} className={styles.workspaceBody}>
          {isLoading ? (
            <Text color="grey">
              <FormattedMessage id="cloud.contextLayer.workspace.connectorsLoading" />
            </Text>
          ) : !hasError && connectors.length === 0 ? (
            <Text color="grey">
              <FormattedMessage id="cloud.contextLayer.workspace.noKindConnectors" values={{ actorKind }} />
            </Text>
          ) : (
            renderConnectorSection(
              actorKind === "source"
                ? "cloud.contextLayer.workspace.sources"
                : "cloud.contextLayer.workspace.destinations",
              connectors,
              actorKind,
              enabledCount,
              hasError
            )
          )}
        </div>
      )}
    </div>
  );
};

const WorkspaceConnectorAccess: React.FC<{
  workspacesQuery: ReturnType<typeof useListWorkspacesInOrganization>;
  actorKind: ActorKind;
}> = ({ workspacesQuery, actorKind }) => {
  const [enabledConnectors, setEnabledConnectors] = useState<Record<string, boolean>>({});
  const [connectorInventories, setConnectorInventories] = useState<
    Record<string, { count: number | null; canManageConnectors: boolean }>
  >({});
  const [visibleWorkspaceCount, setVisibleWorkspaceCount] = useState(25);
  const navigate = useNavigate();
  const organizationId = useCurrentOrganizationId();
  const [organizationWorkspaceMap] = useLocalStorage("airbyte_organization-workspace-map", {});
  const { fetchNextPage, hasNextPage, isFetchingNextPage, isError } = workspacesQuery;
  const onInventoryLoaded = useCallback((workspaceId: string, count: number | null, canManageConnectors: boolean) => {
    setConnectorInventories((current) =>
      current[workspaceId]?.count === count && current[workspaceId]?.canManageConnectors === canManageConnectors
        ? current
        : { ...current, [workspaceId]: { count, canManageConnectors } }
    );
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
    workspaces.length > 0 && workspaces.every((workspace) => workspace.workspaceId in connectorInventories);
  const isEmpty =
    inventoriesLoaded &&
    !hasMoreWorkspaces &&
    !isFetchingNextPage &&
    workspaces.every((workspace) => connectorInventories[workspace.workspaceId].count === 0);
  const storedWorkspaceId = organizationWorkspaceMap[organizationId];
  const targetWorkspaceId =
    workspaces.find(
      (workspace) =>
        workspace.workspaceId === storedWorkspaceId && connectorInventories[workspace.workspaceId]?.canManageConnectors
    )?.workspaceId ??
    workspaces.find((workspace) => connectorInventories[workspace.workspaceId]?.canManageConnectors)?.workspaceId;

  return (
    <div className={styles.workspaceSection}>
      <Heading as="h2" size="sm">
        <FormattedMessage
          id={actorKind === "source" ? "cloud.contextLayer.sources.title" : "cloud.contextLayer.destinations.title"}
        />
      </Heading>
      <Text className={styles.workspaceDescription}>
        <FormattedMessage
          id={
            actorKind === "source"
              ? "cloud.contextLayer.sources.description"
              : "cloud.contextLayer.destinations.description"
          }
        />
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
              text={
                <FormattedMessage
                  id={
                    actorKind === "source"
                      ? "cloud.contextLayer.sources.empty.title"
                      : "cloud.contextLayer.destinations.empty.title"
                  }
                />
              }
              description={
                <FormattedMessage
                  id={
                    actorKind === "source"
                      ? "cloud.contextLayer.sources.empty.description"
                      : "cloud.contextLayer.destinations.empty.description"
                  }
                />
              }
              button={
                targetWorkspaceId && (
                  <Button
                    type="button"
                    variant="primary"
                    className={styles.emptyStateButton}
                    onClick={() =>
                      navigate(
                        `/${RoutePaths.Workspaces}/${targetWorkspaceId}/${
                          actorKind === "source" ? RoutePaths.Source : RoutePaths.Destination
                        }/${
                          actorKind === "source" ? SourcePaths.SelectSourceNew : DestinationPaths.SelectDestinationNew
                        }`
                      )
                    }
                  >
                    <FormattedMessage
                      id={
                        actorKind === "source"
                          ? "cloud.contextLayer.sources.empty.add"
                          : "cloud.contextLayer.destinations.empty.add"
                      }
                    />
                  </Button>
                )
              }
            />
          )}
          {workspaces.map((workspace) => (
            <div key={workspace.workspaceId} hidden={isEmpty || !(workspace.workspaceId in connectorInventories)}>
              <WorkspaceConnectorCard
                workspace={workspace}
                enabledConnectors={enabledConnectors}
                actorKind={actorKind}
                onInventoryLoaded={onInventoryLoaded}
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

const ContextLayerToggle: React.FC<{ enabled: boolean; onClick?: () => void }> = ({ enabled, onClick }) => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.toggleRow}>
      <div className={styles.toggleText}>
        <Text size="sm" bold>
          <FormattedMessage id="cloud.contextLayer.toggle.label" />
        </Text>
        <Text size="sm" color="grey">
          <FormattedMessage id="cloud.contextLayer.toggle.description" />
        </Text>
      </div>
      <div className={styles.toggleControl}>
        <Text size="sm" color="grey">
          <FormattedMessage id={enabled ? "cloud.contextLayer.toggle.enabled" : "cloud.contextLayer.toggle.disabled"} />
        </Text>
        <Switch
          size="sm"
          checked={enabled}
          disabled={!onClick}
          onChange={onClick ? () => onClick() : undefined}
          aria-label={formatMessage({ id: "cloud.contextLayer.toggle.label" })}
        />
      </div>
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
            <ExternalLink href={links.agentsDocs} opensInNewTab>
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
  const { openModal } = useModalService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });
  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: 25, rowOffset: 0 },
    enabled: isEligible,
  });
  const [isOpeningModal, setIsOpeningModal] = useState(false);

  if (isCloudApp && statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (isCloudApp && statusQuery.isError) {
    return <ContextLayerLoadError onRetry={() => statusQuery.refetch()} />;
  }
  if (!isCloudApp || !isEligible || !status) {
    return <ContextLayerUnavailable />;
  }

  const openTermsModal = () => {
    let enrolled = false;
    let retryActors: Parameters<typeof enableWorkspaceActors.mutateAsync>[0]["retryActors"];
    setIsOpeningModal(true);
    void openModal({
      title: formatMessage({ id: "cloud.contextLayer.terms.title" }),
      size: "xl",
      testId: "context-layer-tos-modal",
      content: ({ onCancel, onComplete }) => (
        <ContextLayerTosModal
          onCancel={onCancel}
          onComplete={() => onComplete(undefined)}
          onAccept={async () => {
            let workspaceData = workspacesQuery.data;
            let hasNextPage = workspacesQuery.hasNextPage;

            if (!workspaceData) {
              const refreshed = await workspacesQuery.refetch();
              workspaceData = refreshed.data ?? workspaceData;
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
              workspaceData?.pages.flatMap((page) => page.workspaces ?? []).map((workspace) => workspace.workspaceId) ??
              [];

            if (workspaceIds.length === 0) {
              throw new Error("No workspaces available for enrollment");
            }
            if (!enrolled) {
              await enrollOrganization.mutateAsync({ workspaceIds, addAllSupportedActors: false });
              enrolled = true;
            }
            try {
              retryActors = await enableWorkspaceActors.mutateAsync({ workspaceIds, retryActors });
            } catch (error) {
              retryActors = undefined;
              throw error;
            }
            if (retryActors.length > 0) {
              throw new Error("Connector enablement failed");
            }
          }}
        />
      ),
    }).finally(() => setIsOpeningModal(false));
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
            <FormattedMessage id="cloud.contextLayer.subtitle" />
          </Text>
        </div>
        <Card
          title={formatMessage({
            id: status.is_enrolled ? "cloud.contextLayer.status.title" : "cloud.contextLayer.enable.title",
          })}
          dataTestId="context-layer-card"
        >
          <div className={styles.cardContent}>
            <Text>
              <FormattedMessage
                id={
                  status.is_enrolled ? "cloud.contextLayer.status.description" : "cloud.contextLayer.enable.description"
                }
              />
            </Text>
            <ContextLayerToggle
              enabled={status.is_enrolled}
              onClick={
                canManageOrganizationPermissions
                  ? status.is_enrolled
                    ? openDisableConfirmation
                    : openTermsModal
                  : undefined
              }
            />
            {!status.is_enrolled && (
              <>
                <div className={styles.infoBox}>
                  <Text size="sm">
                    <FormattedMessage id="cloud.contextLayer.enable.info" />
                  </Text>
                </div>
                {canManageOrganizationPermissions ? (
                  <Button
                    type="button"
                    variant="primaryDark"
                    isLoading={isOpeningModal}
                    onClick={openTermsModal}
                    data-testid="context-layer-enable-button"
                  >
                    <FormattedMessage id="cloud.contextLayer.enable.button" />
                  </Button>
                ) : (
                  <div data-testid="context-layer-admin-required">
                    <Text color="grey">
                      <FormattedMessage id="cloud.contextLayer.enable.adminRequired" />
                    </Text>
                    <Text color="grey" size="sm">
                      <FormattedMessage id="cloud.contextLayer.enable.adminRequired.description" />
                    </Text>
                  </div>
                )}
              </>
            )}
            {status.is_enrolled && (
              <ExternalLink href={links.agentsDocs} opensInNewTab>
                <FormattedMessage id="cloud.contextLayer.docs" />
              </ExternalLink>
            )}
          </div>
        </Card>
      </FlexContainer>
    </div>
  );
};

const ContextLayerConnectorsPageContent: React.FC<{ actorKind: ActorKind }> = ({ actorKind }) => {
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
          <Heading as="h2" size="sm">
            <FormattedMessage
              id={actorKind === "source" ? "cloud.contextLayer.sources.title" : "cloud.contextLayer.destinations.title"}
            />
          </Heading>
          <Text className={styles.workspaceDescription}>
            <FormattedMessage
              id={
                actorKind === "source"
                  ? "cloud.contextLayer.sources.description"
                  : "cloud.contextLayer.destinations.noAccess.pageDescription"
              }
            />
          </Text>
          <EmptyState
            icon="aiStars"
            text={
              <FormattedMessage
                id={
                  actorKind === "source"
                    ? "cloud.contextLayer.sources.noAccess.title"
                    : "cloud.contextLayer.destinations.noAccess.title"
                }
              />
            }
            description={
              <FormattedMessage
                id={
                  actorKind === "source"
                    ? "cloud.contextLayer.sources.noAccess.description"
                    : "cloud.contextLayer.destinations.noAccess.description"
                }
              />
            }
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
      <WorkspaceConnectorAccess
        key={`${organizationId}:${actorKind}`}
        workspacesQuery={workspacesQuery}
        actorKind={actorKind}
      />
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

export const ContextLayerConnectorsPage: React.FC<{ actorKind: ActorKind }> = ({ actorKind }) => {
  return (
    <React.Suspense fallback={<LoadingPage />}>
      <ContextLayerConnectorsPageContent actorKind={actorKind} />
    </React.Suspense>
  );
};
