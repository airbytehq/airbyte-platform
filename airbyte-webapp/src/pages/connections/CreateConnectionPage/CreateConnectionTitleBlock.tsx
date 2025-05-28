import { Fragment, Suspense } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useLocation, useSearchParams } from "react-router-dom";

import { ConnectorIcon } from "components/ConnectorIcon";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { NumberBadge } from "components/ui/NumberBadge";
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

type StepStatus = "complete" | "active" | "incomplete";
const COMPLETE = "complete";
const ACTIVE = "active";
const INCOMPLETE = "incomplete";
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
      defineSource: ACTIVE,
      defineDestination: INCOMPLETE,
      selectStreams: INCOMPLETE,
      configureConnection: INCOMPLETE,
    };
  }

  if (source && !destination) {
    return {
      defineSource: COMPLETE,
      defineDestination: ACTIVE,
      selectStreams: INCOMPLETE,
      configureConnection: INCOMPLETE,
    };
  }
  if (destination && !source) {
    return {
      defineSource: ACTIVE,
      defineDestination: COMPLETE,
      selectStreams: INCOMPLETE,
      configureConnection: INCOMPLETE,
    };
  }
  if (source && destination) {
    return {
      defineSource: COMPLETE,
      defineDestination: COMPLETE,
      selectStreams: isOnContinuedSimplifiedStep ? COMPLETE : ACTIVE,
      configureConnection: isOnContinuedSimplifiedStep ? ACTIVE : INCOMPLETE,
    };
  }
  return undefined;
};

const StepItem: React.FC<{ state: StepStatus; step: keyof ConnectionSteps; value: number }> = ({
  state,
  step,
  value,
}) => {
  const location = useLocation();
  const color = state === INCOMPLETE ? "grey" : "blue";
  const isDataActivation = location.pathname.includes(ConnectionRoutePaths.ConfigureDataActivation);
  const messageId =
    step === "defineSource"
      ? "connectionForm.defineSource"
      : step === "defineDestination"
      ? "connectionForm.defineDestination"
      : step === "configureConnection"
      ? "connectionForm.configureConnection"
      : isDataActivation
      ? "connection.create.mapFields"
      : "connectionForm.selectStreams";

  return (
    <FlexContainer alignItems="center" gap="sm">
      <NumberBadge value={value} outline={state !== ACTIVE} color={color} />
      <Text color={color} size="sm">
        <FormattedMessage id={messageId} />
      </Text>
    </FlexContainer>
  );
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

  const sourceId = searchParams.get(SOURCEID_PARAM);
  const destinationId = searchParams.get(DESTINATIONID_PARAM);

  const stepStatuses = useCalculateStepStatuses(sourceId, destinationId);
  if (!stepStatuses) {
    // this should not be a possible state, but we'll handle it just in case
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}`} />;
  }

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
        <FlexContainer gap="sm" alignItems="center">
          {(Object.keys(stepStatuses) as Array<keyof ConnectionSteps>).map((step, idx) => {
            return (
              <Fragment key={step}>
                <StepItem state={stepStatuses[step]} step={step} value={idx + 1} />
                {idx !== Object.keys(stepStatuses).length - 1 && <Icon type="chevronRight" color="disabled" />}
              </Fragment>
            );
          })}
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
