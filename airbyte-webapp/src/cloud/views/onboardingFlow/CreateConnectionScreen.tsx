import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { ScrollParent } from "components/ui/ScrollParent";
import { Text } from "components/ui/Text";

import { useCreateConnection, useFirstConnectionBetween } from "core/api";

import { ConnectionPairHeading } from "./ConnectionPairHeading";
import styles from "./CreateConnectionScreen.module.scss";
import { FIRST_CONNECTION_FORM_ID, FirstConnectionForm } from "./FirstConnectionForm";
import { OnboardingActionBar } from "./OnboardingActionBar";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { OnboardingPair, useOnboardingPair } from "./useOnboardingPair";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";

/** The stream table needs a scroll container of its own, so the screen fills the page body and scrolls inside it. */
const CreateConnectionLayout: React.FC<React.PropsWithChildren<{ actions: React.ReactNode }>> = ({
  actions,
  children,
}) => (
  <ScrollParent>
    <div className={styles.content}>
      <OnboardingScreenLayout title={<FormattedMessage id="onboardingFlow.createConnection.title" />} actions={actions}>
        <Text size="lg" color="grey">
          <FormattedMessage id="onboardingFlow.createConnection.description" />
        </Text>
        {children}
      </OnboardingScreenLayout>
    </div>
  </ScrollParent>
);

const PairCard: React.FC<React.PropsWithChildren<{ pair: OnboardingPair }>> = ({ pair, children }) => (
  <Card>
    <FlexContainer direction="column" gap="lg">
      <ConnectionPairHeading pair={pair} as="h2" size="sm" />
      {children}
    </FlexContainer>
  </Card>
);

interface PairConnectionStepProps {
  pair: OnboardingPair;
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
  isSaving: boolean;
}

/**
 * The flow's connection is the first connection from the pair's source to its destination. When it exists, for example
 * after Back from "Run your first sync", Continue moves on without a second connection.
 */
const PairConnectionStep: React.FC<PairConnectionStepProps> = ({ pair, onBack, onContinue, onSkip, isSaving }) => {
  const connection = useFirstConnectionBetween(pair.source.sourceId, pair.destination.destinationId);
  const { mutateAsync: createConnection, isLoading: isCreating } = useCreateConnection();

  if (connection) {
    return (
      <CreateConnectionLayout
        actions={<OnboardingActionBar onSkip={onSkip} onBack={onBack} onContinue={onContinue} isSaving={isSaving} />}
      >
        <PairCard pair={pair}>
          <Text size="lg">
            <FormattedMessage id="onboardingFlow.createConnection.ready" />
          </Text>
        </PairCard>
      </CreateConnectionLayout>
    );
  }
  return (
    <CreateConnectionLayout
      actions={
        <OnboardingActionBar
          onSkip={onSkip}
          onBack={onBack}
          continueFormId={FIRST_CONNECTION_FORM_ID}
          isSaving={isCreating || isSaving}
        />
      }
    >
      <PairCard pair={pair}>
        <Text size="lg">
          <FormattedMessage id="onboardingFlow.createConnection.streams" />
        </Text>
        <FirstConnectionForm pair={pair} createConnection={createConnection} onCreated={onContinue} />
      </PairCard>
    </CreateConnectionLayout>
  );
};

interface CreateConnectionScreenProps {
  sourcePicks: string[];
  destinationChoice?: string;
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
}

/** Back returns to the destination, and a created connection moves on to "Run your first sync". Each saves its status first. */
export const CreateConnectionScreen: React.FC<CreateConnectionScreenProps> = ({
  sourcePicks,
  destinationChoice,
  onBack,
  onContinue,
  onSkip,
}) => {
  const pair = useOnboardingPair(sourcePicks, destinationChoice);
  const { saveStatus, isSaving } = useSaveOnboardingStatus();
  const goBack = () => saveStatus("create_destination", onBack);
  const goToFirstSync = () => saveStatus("run_first_sync", onContinue);

  if (!pair) {
    return (
      <CreateConnectionLayout actions={<OnboardingActionBar onSkip={onSkip} onBack={goBack} isSaving={isSaving} />}>
        <Text size="lg">
          <FormattedMessage id="onboardingFlow.createConnection.missing" />
        </Text>
      </CreateConnectionLayout>
    );
  }
  return (
    <PairConnectionStep pair={pair} onBack={goBack} onContinue={goToFirstSync} onSkip={onSkip} isSaving={isSaving} />
  );
};
