import React, { useMemo, useState } from "react";
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
import { useAgentsProvisioningStatus, useEnrollOrganizationInAgents, useListWorkspacesInOrganization } from "core/api";
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
  enabledByDefault: boolean;
}

interface WorkspaceConnectorData {
  workspaceId: string;
  workspaceName: string;
  sources: Connector[];
  destinations: Connector[];
}

const DEMO_CONNECTORS = {
  sources: [
    { id: "github", name: "GitHub", enabledByDefault: true },
    { id: "stripe", name: "Stripe", enabledByDefault: true },
    { id: "salesforce", name: "Salesforce", enabledByDefault: true },
  ],
  destinations: [
    { id: "postgres", name: "Postgres", enabledByDefault: false },
    { id: "bigquery", name: "BigQuery", enabledByDefault: false },
  ],
};

const WorkspaceConnectorCard: React.FC<{
  workspace: WorkspaceConnectorData;
  enabledConnectors: Record<string, boolean>;
  onToggle: (key: string, enabled: boolean) => void;
}> = ({ workspace, enabledConnectors, onToggle }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const sourceCount = workspace.sources.filter(
    (connector) => enabledConnectors[`${workspace.workspaceId}:${connector.id}`] ?? connector.enabledByDefault
  ).length;
  const destinationCount = workspace.destinations.filter(
    (connector) => enabledConnectors[`${workspace.workspaceId}:${connector.id}`] ?? connector.enabledByDefault
  ).length;
  const connectorRows = (connectors: Connector[]) =>
    connectors.map((connector) => {
      const key = `${workspace.workspaceId}:${connector.id}`;
      const checked = enabledConnectors[key] ?? connector.enabledByDefault;
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
            onChange={(event) => onToggle(key, event.target.checked)}
            aria-label={connector.name}
          />
        </div>
      );
    });

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
              values={{ sources: workspace.sources.length, destinations: workspace.destinations.length }}
            />
          </Text>
        </div>
      </button>
      {isExpanded && (
        <div id={workspaceBodyId} className={styles.workspaceBody}>
          <div className={styles.connectorSection}>
            <div className={styles.connectorSectionHeader}>
              <Text size="xs" bold smallcaps>
                <FormattedMessage id="cloud.contextLayer.workspace.sources" />
              </Text>
              <span className={styles.countBadge}>
                <FormattedMessage
                  id="cloud.contextLayer.workspace.enabledCount"
                  values={{ enabled: sourceCount, total: workspace.sources.length }}
                />
              </span>
            </div>
            <div className={styles.connectorRows}>{connectorRows(workspace.sources)}</div>
          </div>
          <div className={styles.connectorSection}>
            <div className={styles.connectorSectionHeader}>
              <Text size="xs" bold smallcaps>
                <FormattedMessage id="cloud.contextLayer.workspace.destinations" />
              </Text>
              <span className={styles.countBadge}>
                <FormattedMessage
                  id="cloud.contextLayer.workspace.enabledCount"
                  values={{ enabled: destinationCount, total: workspace.destinations.length }}
                />
              </span>
            </div>
            <div className={styles.connectorRows}>{connectorRows(workspace.destinations)}</div>
          </div>
        </div>
      )}
    </div>
  );
};

const WorkspaceConnectorAccess: React.FC<{
  workspacesQuery: ReturnType<typeof useListWorkspacesInOrganization>;
}> = ({ workspacesQuery }) => {
  const [enabledConnectors, setEnabledConnectors] = useState<Record<string, boolean>>({});

  const workspaces = useMemo<WorkspaceConnectorData[]>(() => {
    const apiWorkspaces = workspacesQuery.data?.pages.flatMap((page) => page.workspaces ?? []) ?? [];
    return apiWorkspaces.map((workspace) => ({
      workspaceId: workspace.workspaceId,
      workspaceName: workspace.name,
      sources: DEMO_CONNECTORS.sources,
      destinations: DEMO_CONNECTORS.destinations,
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
        workspaces.map((workspace) => (
          <WorkspaceConnectorCard
            key={workspace.workspaceId}
            workspace={workspace}
            enabledConnectors={enabledConnectors}
            onToggle={(key, enabled) => setEnabledConnectors((current) => ({ ...current, [key]: enabled }))}
          />
        ))
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
              <ExternalLink href={`${links.agentEngineApp}/organizations/${organizationId}/get-started`} opensInNewTab>
                <FormattedMessage id="cloud.contextLayer.openAgents" />
              </ExternalLink>
            )}
          </div>
        </Card>
        {status.is_enrolled && <WorkspaceConnectorAccess workspacesQuery={workspacesQuery} />}
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
