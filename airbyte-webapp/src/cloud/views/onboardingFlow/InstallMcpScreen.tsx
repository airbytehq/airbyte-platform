import React, { Suspense, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink, Link } from "components/ui/Link";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { CLOUD_MCP_URL } from "cloud/components/AgentsOptIn/mcpInstallConfigs";
import { HttpError } from "core/api";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { RoutePaths } from "pages/routePaths";

import styles from "./InstallMcpScreen.module.scss";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { useSuggestedPair } from "./useSuggestedPair";

/** The assistant tabs and their setup instructions, in display order. */
const ASSISTANTS = [
  {
    id: "claudeDesktop",
    labelId: "onboardingFlow.installMcp.assistant.claudeDesktop",
    instructionId: "onboardingFlow.installMcp.assistant.claudeDesktop.instruction",
  },
  {
    id: "claudeCode",
    labelId: "onboardingFlow.installMcp.assistant.claudeCode",
    instructionId: "onboardingFlow.installMcp.assistant.claudeCode.instruction",
  },
  {
    id: "cursor",
    labelId: "onboardingFlow.installMcp.assistant.cursor",
    instructionId: "onboardingFlow.installMcp.assistant.cursor.instruction",
  },
  {
    id: "vsCode",
    labelId: "onboardingFlow.installMcp.assistant.vsCode",
    instructionId: "onboardingFlow.installMcp.assistant.vsCode.instruction",
  },
  {
    id: "other",
    labelId: "onboardingFlow.installMcp.assistant.other",
    instructionId: "onboardingFlow.installMcp.assistant.other.instruction",
  },
];

interface CopyFieldProps {
  content: string;
  copyLabelId: string;
}

const CopyField: React.FC<React.PropsWithChildren<CopyFieldProps>> = ({ content, copyLabelId, children }) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer alignItems="center" justifyContent="space-between" gap="lg" className={styles.copyField}>
      {children}
      <CopyButton content={content} aria-label={formatMessage({ id: copyLabelId })}>
        <FormattedMessage id="onboardingFlow.installMcp.copy" />
      </CopyButton>
    </FlexContainer>
  );
};

const StepCard: React.FC<React.PropsWithChildren<{ titleId: string }>> = ({ titleId, children }) => (
  <Card>
    <FlexContainer direction="column" gap="lg">
      <Heading as="h2" size="sm">
        <FormattedMessage id={titleId} />
      </Heading>
      {children}
    </FlexContainer>
  </Card>
);

const InstallStep: React.FC = () => {
  const [assistant, setAssistant] = useState(ASSISTANTS[0]);

  return (
    <StepCard titleId="onboardingFlow.installMcp.install.title">
      <Tabs>
        {ASSISTANTS.map((option) => (
          <ButtonTab
            key={option.id}
            id={option.id}
            name={<FormattedMessage id={option.labelId} />}
            isActive={option.id === assistant.id}
            onSelect={() => setAssistant(option)}
          />
        ))}
      </Tabs>
      <Text size="lg" color="grey">
        <FormattedMessage id={assistant.instructionId} />
      </Text>
      <CopyField content={CLOUD_MCP_URL} copyLabelId="onboardingFlow.installMcp.copy.serverUrl">
        <Text size="lg" bold>
          {CLOUD_MCP_URL}
        </Text>
      </CopyField>
    </StepCard>
  );
};

type PairNames = Record<"source" | "destination", string>;

/** The prompt names the pair when the flow knows it, and is general otherwise. */
const FirstPromptStep: React.FC<{ pairNames?: PairNames }> = ({ pairNames }) => {
  const { formatMessage } = useIntl();
  const prompt = pairNames
    ? formatMessage({ id: "onboardingFlow.installMcp.prompt.personalized" }, pairNames)
    : formatMessage({ id: "onboardingFlow.installMcp.prompt.general" });

  return (
    <StepCard titleId="onboardingFlow.installMcp.prompt.title">
      <CopyField content={prompt} copyLabelId="onboardingFlow.installMcp.copy.prompt">
        <Text size="lg">{prompt}</Text>
      </CopyField>
      {pairNames && (
        <Text size="sm" color="grey">
          <FormattedMessage id="onboardingFlow.installMcp.prompt.note" values={pairNames} />
        </Text>
      )}
    </StepCard>
  );
};

interface SuggestedPairPromptStepProps {
  sourcePicks: string[];
  destinationChoice?: string;
}

/** The pair is the suggested pair of the `chooseMethod` screen. */
const SuggestedPairPromptStep: React.FC<SuggestedPairPromptStepProps> = ({ sourcePicks, destinationChoice }) => {
  const pair = useSuggestedPair(sourcePicks, destinationChoice);
  return <FirstPromptStep pairNames={pair && { source: pair.source.name, destination: pair.destination.name }} />;
};

/**
 * A failed definition or spec request shows the general prompt, as when no pair is known, so the member still has a
 * first prompt. The API call already reports an HttpError, so this boundary reports only other errors, as
 * DefaultErrorBoundary does.
 */
class GeneralPromptOnError extends React.Component<React.PropsWithChildren, { hasError: boolean }> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  override componentDidCatch(error: Error) {
    if (!(error instanceof HttpError)) {
      trackError(error);
    }
  }

  override render() {
    return this.state.hasError ? <FirstPromptStep /> : this.props.children;
  }
}

interface InstallMcpScreenProps {
  sourcePicks: string[];
  destinationChoice?: string;
}

/**
 * The AI agent path opens this screen after it completes onboarding, so the screen has no Skip or Back, and a reload
 * opens the normal app. Finished opens the normal app.
 */
export const InstallMcpScreen: React.FC<InstallMcpScreenProps> = ({ sourcePicks, destinationChoice }) => {
  const workspaceId = useCurrentWorkspaceId();

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.installMcp.title" />}
      actions={
        <FlexContainer justifyContent="flex-end">
          <Link to={`/${RoutePaths.Workspaces}/${workspaceId}`} variant="buttonPrimary">
            <FormattedMessage id="onboardingFlow.installMcp.finished" />
          </Link>
        </FlexContainer>
      }
    >
      <Text size="lg">
        <ExternalLink href={links.airbyteMcpDocs} opensInNewTab>
          <FormattedMessage id="onboardingFlow.installMcp.docs" />
        </ExternalLink>
      </Text>
      <InstallStep />
      <GeneralPromptOnError>
        <Suspense fallback={<LoadingSkeleton />}>
          <SuggestedPairPromptStep sourcePicks={sourcePicks} destinationChoice={destinationChoice} />
        </Suspense>
      </GeneralPromptOnError>
    </OnboardingScreenLayout>
  );
};
