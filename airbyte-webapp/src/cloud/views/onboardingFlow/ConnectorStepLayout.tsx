import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { ConnectorDocumentationWrapper } from "area/connector/components/ConnectorDocumentationLayout";
import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";

import styles from "./ConnectorStepLayout.module.scss";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";

type ConnectorType = "source" | "destination";

interface ConnectorStepLayoutProps {
  definition: SourceDefinitionRead | DestinationDefinitionRead;
  actions: React.ReactNode;
}

export const ConnectorStepLayout: React.FC<React.PropsWithChildren<ConnectorStepLayoutProps>> = ({
  definition,
  actions,
  children,
}) => (
  <ConnectorDocumentationWrapper footer={<div className={styles.actions}>{actions}</div>}>
    <FlexContainer direction="column" gap="xl" className={styles.content}>
      <FlexContainer alignItems="center" gap="md">
        <ConnectorIcon icon={definition.icon} />
        <Heading as="h1" size="lg">
          {definition.name}
        </Heading>
      </FlexContainer>
      {children}
    </FlexContainer>
  </ConnectorDocumentationWrapper>
);

const MISSING_CONNECTOR_TEXT: Record<ConnectorType, { titleId: string; descriptionId: string }> = {
  source: { titleId: "onboardingFlow.progress.source", descriptionId: "onboardingFlow.connectSource.missing" },
  destination: {
    titleId: "onboardingFlow.progress.destination",
    descriptionId: "onboardingFlow.connectDestination.missing",
  },
};

interface MissingConnectorStepProps {
  connectorType: ConnectorType;
  actions: React.ReactNode;
}

/**
 * Shown when the step has no connector, for example after someone deleted the draft on the Sources page. The step fills
 * the page body, so this sets its own padding.
 */
export const MissingConnectorStep: React.FC<MissingConnectorStepProps> = ({ connectorType, actions }) => {
  const { titleId, descriptionId } = MISSING_CONNECTOR_TEXT[connectorType];

  return (
    <div className={styles.content}>
      <OnboardingScreenLayout title={<FormattedMessage id={titleId} />} actions={actions}>
        <Text size="lg">
          <FormattedMessage id={descriptionId} />
        </Text>
      </OnboardingScreenLayout>
    </div>
  );
};
