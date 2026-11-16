import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { OnboardingActionBar } from "./OnboardingActionBar";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";

interface CreateConnectionScreenProps {
  onBack: () => void;
  onSkip?: () => void;
}

/** Holds the place of the connection setup, so an organization with a set-up destination has a screen. */
export const CreateConnectionScreen: React.FC<CreateConnectionScreenProps> = ({ onBack, onSkip }) => {
  const { saveStatus, isSaving } = useSaveOnboardingStatus();

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.createConnection.title" />}
      actions={
        <OnboardingActionBar
          onSkip={onSkip}
          onBack={() => saveStatus("create_destination", onBack)}
          isSaving={isSaving}
        />
      }
    >
      <Text size="lg">
        <FormattedMessage id="onboardingFlow.createConnection.description" />
      </Text>
    </OnboardingScreenLayout>
  );
};
