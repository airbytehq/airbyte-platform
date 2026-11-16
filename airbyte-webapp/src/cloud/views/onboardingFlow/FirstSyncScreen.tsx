import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon, IconType } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useConnectionStatus } from "area/connection/components/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicator } from "area/connection/components/ConnectionStatusIndicator";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useFirstConnectionBetween, useSyncConnection } from "core/api";
import { ConnectionSyncStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { isDefined } from "core/utils/common";
import { DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

import { ConnectionPairHeading } from "./ConnectionPairHeading";
import styles from "./FirstSyncScreen.module.scss";
import { OnboardingActionBar } from "./OnboardingActionBar";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { OnboardingPair, useOnboardingPair } from "./useOnboardingPair";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";

interface SyncStatusCardProps {
  indicator: React.ReactNode;
  titleId: string;
  description: React.ReactNode;
}

const SyncStatusCard: React.FC<SyncStatusCardProps> = ({ indicator, titleId, description }) => (
  <Card>
    <FlexContainer alignItems="center" gap="lg">
      {indicator}
      <FlexContainer direction="column" gap="xs">
        <Heading as="h2" size="sm">
          <FormattedMessage id={titleId} />
        </Heading>
        <Text size="lg" color="grey">
          {description}
        </Text>
      </FlexContainer>
    </FlexContainer>
  </Card>
);

interface NextStepOptionProps {
  icon: IconType;
  labelId: string;
  linkLabelId: string;
  to: string;
  isPrimary?: boolean;
}

const NextStepOption: React.FC<NextStepOptionProps> = ({ icon, labelId, linkLabelId, to, isPrimary = false }) => (
  <FlexContainer
    alignItems="center"
    gap="lg"
    className={classNames(styles.option, { [styles["option--primary"]]: isPrimary })}
  >
    <span className={styles.iconBox}>
      <Icon type={icon} size="sm" />
    </span>
    <Text size="lg" bold className={styles.optionLabel}>
      <FormattedMessage id={labelId} />
    </Text>
    <Link to={to} variant={isPrimary ? "buttonPrimary" : "button"}>
      <FormattedMessage id={linkLabelId} />
    </Link>
  </FlexContainer>
);

const SYNC_STATE_TEXT = {
  running: {
    titleId: "onboardingFlow.firstSync.running.title",
    descriptionId: "onboardingFlow.firstSync.running.description",
  },
  ended: {
    titleId: "onboardingFlow.firstSync.ended.title",
    descriptionId: "onboardingFlow.firstSync.ended.description",
  },
};

/** The server reports an active job that has not started running yet as queued or pending. */
const ACTIVE_SYNC_STATUSES = new Set<ConnectionSyncStatus>([
  ConnectionSyncStatus.running,
  ConnectionSyncStatus.queued,
  ConnectionSyncStatus.pending,
]);

interface FirstSyncInProgressProps {
  pair: OnboardingPair;
  connectionId: string;
}

/**
 * Each link opens the normal app. Onboarding is complete, so the screen has no Skip or Back. The indicator shows the
 * connection's live status, so it stays honest when the sync ends while the member waits.
 */
const FirstSyncInProgress: React.FC<FirstSyncInProgressProps> = ({ pair, connectionId }) => {
  const createWorkspaceLink = useCurrentWorkspaceLink();
  const organizationId = useCurrentOrganizationId();
  const { status } = useConnectionStatus(connectionId);
  const { titleId, descriptionId } = SYNC_STATE_TEXT[ACTIVE_SYNC_STATUSES.has(status) ? "running" : "ended"];

  return (
    <FlexContainer direction="column" gap="xl">
      <ConnectionPairHeading pair={pair} as="h1" size="lg" />
      <SyncStatusCard
        indicator={<ConnectionStatusIndicator status={status} withBox />}
        titleId={titleId}
        description={
          <FormattedMessage
            id={descriptionId}
            values={{ source: pair.source.sourceName, destination: pair.destination.destinationName }}
          />
        }
      />
      <Heading as="h2" size="sm">
        <FormattedMessage id="onboardingFlow.firstSync.next" />
      </Heading>
      <FlexContainer direction="column" gap="md">
        <NextStepOption
          icon="robot"
          labelId="onboardingFlow.firstSync.next.mcp"
          linkLabelId="onboardingFlow.firstSync.next.mcp.link"
          to={`/${RoutePaths.Organization}/${organizationId}/${CloudSettingsRoutePaths.InstallMcp}`}
          isPrimary
        />
        <NextStepOption
          icon="sync"
          labelId="onboardingFlow.firstSync.next.monitor"
          linkLabelId="onboardingFlow.firstSync.next.monitor.link"
          to={createWorkspaceLink(`/${RoutePaths.Connections}/${connectionId}`)}
        />
        <NextStepOption
          icon="source"
          labelId="onboardingFlow.firstSync.next.sources"
          linkLabelId="onboardingFlow.firstSync.next.sources.link"
          to={createWorkspaceLink(`/${RoutePaths.Source}/${SourcePaths.SelectSourceNew}`)}
        />
        <NextStepOption
          icon="destination"
          labelId="onboardingFlow.firstSync.next.destinations"
          linkLabelId="onboardingFlow.firstSync.next.destinations.link"
          to={createWorkspaceLink(`/${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}`)}
        />
      </FlexContainer>
    </FlexContainer>
  );
};

interface ReadyToSyncStepProps {
  pair: OnboardingPair;
  connection: WebBackendConnectionListItem;
  onBack: () => void;
  onSkip?: () => void;
  onSyncStarted: () => void;
  isGoingBack: boolean;
}

/**
 * Starts the sync first and completes onboarding after it, so a failed start keeps the member on the step. A failed
 * start shows the sync error notification of `useSyncConnection`. The connection has a manual schedule, so an active
 * or finished job means the first sync started: a retry after a failed save then only saves `completed`. The connection
 * status is not used for this, because the server also reports `pending` for a connection that never ran. The card shows
 * a static clock and not the live status, because the step asks the member to start the first sync.
 */
const ReadyToSyncStep: React.FC<ReadyToSyncStepProps> = ({
  pair,
  connection,
  onBack,
  onSkip,
  onSyncStarted,
  isGoingBack,
}) => {
  const { activeJob, lastSyncJobId } = useConnectionStatus(connection.connectionId);
  const { mutate: syncConnection, isLoading: isStarting } = useSyncConnection();
  const { saveStatus, isSaving } = useSaveOnboardingStatus();
  const hasSyncStarted = isDefined(activeJob) || isDefined(lastSyncJobId);
  const completeOnboarding = () => saveStatus("completed", onSyncStarted);
  const runFirstSync = () =>
    hasSyncStarted ? completeOnboarding() : syncConnection(connection, { onSuccess: completeOnboarding });

  return (
    <FlexContainer direction="column" gap="xl">
      <ConnectionPairHeading pair={pair} as="h1" size="lg" />
      <SyncStatusCard
        indicator={
          <span className={styles.iconBox}>
            <Icon type="clockFilled" color="primary" size="md" />
          </span>
        }
        titleId="onboardingFlow.firstSync.ready.title"
        description={<FormattedMessage id="onboardingFlow.firstSync.ready.description" />}
      />
      <OnboardingActionBar
        onSkip={onSkip}
        onBack={onBack}
        onContinue={runFirstSync}
        continueLabel={<FormattedMessage id="onboardingFlow.firstSync.run" />}
        isSaving={isStarting || isSaving || isGoingBack}
      />
    </FlexContainer>
  );
};

interface MissingSetupStepProps {
  descriptionId: string;
  onBack: () => void;
  onSkip?: () => void;
  isGoingBack: boolean;
}

const MissingSetupStep: React.FC<MissingSetupStepProps> = ({ descriptionId, onBack, onSkip, isGoingBack }) => (
  <OnboardingScreenLayout
    title={<FormattedMessage id="onboardingFlow.progress.firstSync" />}
    actions={<OnboardingActionBar onSkip={onSkip} onBack={onBack} isSaving={isGoingBack} />}
  >
    <Text size="lg">
      <FormattedMessage id={descriptionId} />
    </Text>
  </OnboardingScreenLayout>
);

interface PairFirstSyncProps {
  pair: OnboardingPair;
  onBack: () => void;
  onSkip?: () => void;
  isGoingBack: boolean;
}

/**
 * The connection is the one the create connection step made for the pair, so a reload at `run_first_sync` finds it
 * again. After the sync starts, onboarding is complete, and the step shows the running state until the member leaves.
 */
const PairFirstSync: React.FC<PairFirstSyncProps> = ({ pair, onBack, onSkip, isGoingBack }) => {
  const connection = useFirstConnectionBetween(pair.source.sourceId, pair.destination.destinationId);
  const [isSyncStarted, setIsSyncStarted] = useState(false);

  if (!connection) {
    return (
      <MissingSetupStep
        descriptionId="onboardingFlow.firstSync.missing"
        onBack={onBack}
        onSkip={onSkip}
        isGoingBack={isGoingBack}
      />
    );
  }
  if (isSyncStarted) {
    return <FirstSyncInProgress pair={pair} connectionId={connection.connectionId} />;
  }
  return (
    <ReadyToSyncStep
      pair={pair}
      connection={connection}
      onBack={onBack}
      onSkip={onSkip}
      onSyncStarted={() => setIsSyncStarted(true)}
      isGoingBack={isGoingBack}
    />
  );
};

interface FirstSyncScreenProps {
  sourcePicks: string[];
  destinationChoice?: string;
  onBack: () => void;
  onSkip?: () => void;
}

/** Back returns to the create connection step and saves its status first. */
export const FirstSyncScreen: React.FC<FirstSyncScreenProps> = ({ sourcePicks, destinationChoice, onBack, onSkip }) => {
  const pair = useOnboardingPair(sourcePicks, destinationChoice);
  const { saveStatus, isSaving: isGoingBack } = useSaveOnboardingStatus();
  const goBack = () => saveStatus("create_connection", onBack);

  if (!pair) {
    return (
      <MissingSetupStep
        descriptionId="onboardingFlow.createConnection.missing"
        onBack={goBack}
        onSkip={onSkip}
        isGoingBack={isGoingBack}
      />
    );
  }
  return <PairFirstSync pair={pair} onBack={goBack} onSkip={onSkip} isGoingBack={isGoingBack} />;
};
