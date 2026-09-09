import React, { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Message } from "components/ui/Message";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useAgentsProvisioningStatus,
  useEnrollOrganizationInAgents,
  useExternalWorkspaceConnectors,
  useListWorkspacesInOrganization,
  useSetExternalActorEnabled,
} from "core/api";
import { useModalService } from "core/services/Modal";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./ContextLayerPage.module.scss";
import { ContextLayerTosModal } from "./ContextLayerTosModal";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface Connector {
  id: string;
  name: string;
  enabled: boolean;
  supported: boolean;
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
  const { sources, destinations, isLoading, sourcesError, destinationsError } = useExternalWorkspaceConnectors(
    workspace.workspaceId
  );
  const { mutateAsync: setExternalActorEnabled } = useSetExternalActorEnabled();
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
            disabled={!canManageOrganizationPermissions || !connector.supported || pendingConnectors[key]}
            onChange={
              canManageOrganizationPermissions
                ? async (event) => {
                    const enabled = event.target.checked;
                    setPendingConnectors((current) => ({ ...current, [key]: true }));
                    onToggle(key, enabled);
                    try {
                      const result = await setExternalActorEnabled({
                        actorId: connector.id,
                        actorKind,
                        enabled,
                      });
                      onToggle(key, result.enabled);
                      onClearOptimistic(key);
                    } catch {
                      onClearOptimistic(key);
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

const BillingCallout: React.FC = () => (
  <Message
    className={styles.callout}
    type="info"
    text={
      <>
        <FormattedMessage id="cloud.contextLayer.billing" />{" "}
        <ExternalLink href="https://airbyte.com/pricing" opensInNewTab>
          <FormattedMessage id="cloud.contextLayer.pricing" />
        </ExternalLink>
      </>
    }
  />
);

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
          disabled={enabled || !onClick}
          onChange={onClick ? () => onClick() : undefined}
          aria-label={formatMessage({ id: "cloud.contextLayer.toggle.label" })}
        />
      </div>
    </div>
  );
};

const ContextLayerPageContent: React.FC<{ showAgentsOptIn: boolean }> = ({ showAgentsOptIn }) => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const enrollOrganization = useEnrollOrganizationInAgents();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });
  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: 25, rowOffset: 0 },
    enabled: Boolean(status && (status.is_enrolled || status.external_cloud_eligible)),
  });
  const [isOpeningModal, setIsOpeningModal] = useState(false);

  if (!isCloudApp || !status || (!status.is_enrolled && !status.external_cloud_eligible)) {
    return null;
  }

  const openTermsModal = () => {
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
            await enrollOrganization.mutateAsync({ workspaceIds, addAllSupportedActors: true });
          }}
        />
      ),
    }).finally(() => setIsOpeningModal(false));
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
            <BillingCallout />
            <ContextLayerToggle
              enabled={status.is_enrolled}
              onClick={status.is_enrolled || !canManageOrganizationPermissions ? undefined : openTermsModal}
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

  if (!showAgentsOptIn) {
    return null;
  }

  return (
    <React.Suspense fallback={<LoadingPage />}>
      <ContextLayerPageContent showAgentsOptIn={showAgentsOptIn} />
    </React.Suspense>
  );
};
