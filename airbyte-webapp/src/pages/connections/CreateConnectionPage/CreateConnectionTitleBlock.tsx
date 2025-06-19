import { Suspense } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useLocation, useSearchParams } from "react-router-dom";

import { ConnectorIcon } from "components/ConnectorIcon";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { StepStatus, StepsIndicators } from "components/ui/StepsIndicators/StepsIndicators";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import {
  useCurrentWorkspace,
  useDestinationDefinitionVersion,
  useSourceDefinitionVersion,
  useSourceDefinition,
  useDestinationDefinition,
  useGetDestination,
  useGetSource,
} from "core/api";
import { SupportLevel } from "core/api/types/AirbyteClient";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./CreateConnectionTitleBlock.module.scss";

export const SOURCEID_PARAM = "sourceId";
export const DESTINATIONID_PARAM = "destinationId";

interface ConnectionSteps {
  defineSource: StepStatus;
  defineDestination: StepStatus;
  selectStreams: StepStatus;
  configureConnection: StepStatus;
}

const useCalculateStepStatuses = (source: string | null, destination: string | null): ConnectionSteps | undefined => {
  const location = useLocation();
  const isOnContinuedSimplifiedStep = location.pathname.endsWith("/continued");

  if (!source && !destination) {
    return {
      defineSource: StepStatus.ACTIVE,
      defineDestination: StepStatus.INCOMPLETE,
      selectStreams: StepStatus.INCOMPLETE,
      configureConnection: StepStatus.INCOMPLETE,
    };
  }

  if (source && !destination) {
    return {
      defineSource: StepStatus.COMPLETE,
      defineDestination: StepStatus.ACTIVE,
      selectStreams: StepStatus.INCOMPLETE,
      configureConnection: StepStatus.INCOMPLETE,
    };
  }
  if (destination && !source) {
    return {
      defineSource: StepStatus.ACTIVE,
      defineDestination: StepStatus.COMPLETE,
      selectStreams: StepStatus.INCOMPLETE,
      configureConnection: StepStatus.INCOMPLETE,
    };
  }
  if (source && destination) {
    return {
      defineSource: StepStatus.COMPLETE,
      defineDestination: StepStatus.COMPLETE,
      selectStreams: isOnContinuedSimplifiedStep ? StepStatus.COMPLETE : StepStatus.ACTIVE,
      configureConnection: isOnContinuedSimplifiedStep ? StepStatus.ACTIVE : StepStatus.INCOMPLETE,
    };
  }
  return undefined;
};

interface ConnectorBrandingProps {
  icon?: string;
  name?: string;
  supportLevel?: SupportLevel;
  custom?: boolean;
}

const ConnectorBranding: React.FC<ConnectorBrandingProps> = ({ icon, name, supportLevel, custom }) => {
  return (
    <FlexContainer alignItems="center" gap="sm">
      <ConnectorIcon icon={icon} />
      <Text>{name}</Text>
      <SupportLevelBadge supportLevel={supportLevel} custom={custom} />
    </FlexContainer>
  );
};

const SourceBlock: React.FC<{ sourceId: string }> = ({ sourceId }) => {
  return (
    <Suspense fallback={<ConnectorPlaceholder />}>
      <SourceBlockContent sourceId={sourceId} />
    </Suspense>
  );
};

const SourceBlockContent: React.FC<{ sourceId: string }> = ({ sourceId }) => {
  const source = useGetSource(sourceId);
  const sourceDefinition = useSourceDefinition(source?.sourceDefinitionId);
  const sourceDefinitionVersion = useSourceDefinitionVersion(source?.sourceId);

  return (
    <ConnectorBranding
      icon={source?.icon}
      name={source?.name}
      supportLevel={sourceDefinitionVersion?.supportLevel}
      custom={sourceDefinition?.custom}
    />
  );
};

const DestinationBlock: React.FC<{ destinationId: string }> = ({ destinationId }) => {
  return (
    <Suspense fallback={<ConnectorPlaceholder />}>
      <DestinationBlockContent destinationId={destinationId} />
    </Suspense>
  );
};

const DestinationBlockContent: React.FC<{ destinationId: string }> = ({ destinationId }) => {
  const destination = useGetDestination(destinationId);
  const destinationDefinition = useDestinationDefinition(destination?.destinationDefinitionId);
  const destinationDefinitionVersion = useDestinationDefinitionVersion(destination?.destinationId);

  if (!destination) {
    return <ConnectorPlaceholder />;
  }

  return (
    <ConnectorBranding
      icon={destination?.icon}
      name={destination?.name}
      supportLevel={destinationDefinitionVersion?.supportLevel}
      custom={destinationDefinition?.custom}
    />
  );
};

const ConnectorPlaceholder: React.FC = () => {
  return (
    <FlexContainer alignItems="center" gap="sm">
      <div className={styles.iconPlaceholder} />
      <div className={styles.namePlaceholder} />
    </FlexContainer>
  );
};

export const CreateConnectionTitleBlock: React.FC = () => {
  const [searchParams] = useSearchParams();
  const { workspaceId } = useCurrentWorkspace();
  const location = useLocation();
  const isDataActivation = location.pathname.includes(ConnectionRoutePaths.ConfigureDataActivation);

  const sourceId = searchParams.get(SOURCEID_PARAM);
  const destinationId = searchParams.get(DESTINATIONID_PARAM);

  const stepStatuses = useCalculateStepStatuses(sourceId, destinationId);
  if (!stepStatuses) {
    // this should not be a possible state, but we'll handle it just in case
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}`} />;
  }

  const stepLabels: Record<keyof ConnectionSteps, React.ReactNode> = {
    defineSource: <FormattedMessage id="connectionForm.defineSource" />,
    defineDestination: <FormattedMessage id="connectionForm.defineDestination" />,
    selectStreams: isDataActivation ? (
      <FormattedMessage id="connection.create.mapFields" />
    ) : (
      <FormattedMessage id="connectionForm.selectStreams" />
    ),
    configureConnection: <FormattedMessage id="connectionForm.configureConnection" />,
  };

  const steps = (Object.keys(stepStatuses) as Array<keyof ConnectionSteps>).map((step) => ({
    state: stepStatuses[step],
    label: stepLabels[step],
  }));

  return (
    <Box pb="lg">
      <FlexContainer direction="column" gap="xl">
        {(sourceId || destinationId) && (
          <FlexContainer alignItems="center">
            {sourceId && <SourceBlock sourceId={sourceId} />}
            <Icon type="arrowRight" />
            {destinationId && <DestinationBlock destinationId={destinationId} />}
          </FlexContainer>
        )}
        <StepsIndicators steps={steps} />
      </FlexContainer>
    </Box>
  );
};
