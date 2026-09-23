import React, { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
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

const WorkspaceConnectorCard: React.FC<{
  workspace: WorkspaceConnectorData;
  enabledConnectors: Record<string, boolean>;
  onToggle: (key: string, enabled: boolean) => void;
  onClearOptimistic: (key: string) => void;
  canManageOrganizationPermissions: boolean;
}> = ({ workspace, enabledConnectors, onToggle, onClearOptimistic, canManageOrganizationPermissions }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [pendingConnectors, setPendingConnectors] = useState<Record<string, boolean>>({});
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { sources, destinations, isLoading, sourcesError, destinationsError } = useFusionWorkspaceConnectors(
    workspace.workspaceId,
    { hydrate: isExpanded }
  );
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const confirmDisable = useConfirmContextLayerDisable();
  const sourceCount = sources.filter(
    (connector) =>
      connector.supported && (enabledConnectors[`${workspace.workspaceId}:${connector.id}`] ?? connector.enabled)
  ).length;
  const destinationCount = destinations.filter(
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
              !canManageOrganizationPermissions ||
              !connector.supported ||
              connector.loading ||
              connector.error ||
              pendingConnectors[key]
            }
            onChange={
              canManageOrganizationPermissions
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
              id="cloud.contextLayer.workspace.counts"
              values={{ sources: sources.length, destinations: destinations.length }}
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
          ) : !sourcesError && !destinationsError && sources.length === 0 && destinations.length === 0 ? (
            <Text color="grey">
              <FormattedMessage id="cloud.contextLayer.workspace.noConnectors" />
            </Text>
          ) : (
            <>
              {(sourcesError || sources.length > 0) &&
                renderConnectorSection(
                  "cloud.contextLayer.workspace.sources",
                  sources,
                  "source",
                  sourceCount,
                  sourcesError
                )}
              {(destinationsError || destinations.length > 0) &&
                renderConnectorSection(
                  "cloud.contextLayer.workspace.destinations",
                  destinations,
                  "destination",
                  destinationCount,
                  destinationsError
                )}
            </>
          )}
        </div>
      )}
    </div>
  );
};

const WorkspaceConnectorAccess: React.FC<{
  workspacesQuery: ReturnType<typeof useListWorkspacesInOrganization>;
  canManageOrganizationPermissions: boolean;
}> = ({ workspacesQuery, canManageOrganizationPermissions }) => {
  const [enabledConnectors, setEnabledConnectors] = useState<Record<string, boolean>>({});
  const { fetchNextPage, hasNextPage, isFetchingNextPage } = workspacesQuery;

  useEffect(() => {
    if (hasNextPage && !isFetchingNextPage) {
      void fetchNextPage();
    }
  }, [fetchNextPage, hasNextPage, isFetchingNextPage]);

  const workspaces = useMemo<WorkspaceConnectorData[]>(() => {
    const apiWorkspaces = workspacesQuery.data?.pages.flatMap((page) => page.workspaces ?? []) ?? [];
    return apiWorkspaces.map((workspace) => ({
      workspaceId: workspace.workspaceId,
      workspaceName: workspace.name,
    }));
  }, [workspacesQuery.data?.pages]);

  return (
    <div className={styles.workspaceSection}>
      <Heading as="h2" size="sm">
        <FormattedMessage id="cloud.contextLayer.workspace.title" />
      </Heading>
      <Text className={styles.workspaceDescription}>
        <FormattedMessage id="cloud.contextLayer.workspace.description" />
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
          {workspaces.map((workspace) => (
            <WorkspaceConnectorCard
              key={workspace.workspaceId}
              workspace={workspace}
              enabledConnectors={enabledConnectors}
              canManageOrganizationPermissions={canManageOrganizationPermissions}
              onToggle={(key, enabled) => setEnabledConnectors((current) => ({ ...current, [key]: enabled }))}
              onClearOptimistic={(key) =>
                setEnabledConnectors((current) => {
                  const next = { ...current };
                  delete next[key];
                  return next;
                })
              }
            />
          ))}
          {isFetchingNextPage && (
            <Text color="grey">
              <FormattedMessage id="cloud.contextLayer.workspace.loadingMore" />
            </Text>
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

const ContextLayerPageContent: React.FC<{ showAgentsOptIn: boolean }> = ({ showAgentsOptIn }) => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
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
        {status.is_enrolled && (
          <WorkspaceConnectorAccess
            workspacesQuery={workspacesQuery}
            canManageOrganizationPermissions={canManageOrganizationPermissions}
          />
        )}
      </FlexContainer>
    </div>
  );
};

export const ContextLayerPage: React.FC = () => {
  const showAgentsOptIn = useShowAgentsOptIn();

  return (
    <React.Suspense fallback={<LoadingPage />}>
      <ContextLayerPageContent showAgentsOptIn={showAgentsOptIn} />
    </React.Suspense>
  );
};
