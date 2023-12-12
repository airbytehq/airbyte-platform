import { Fragment, Suspense } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useSearchParams } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
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
import { RoutePaths } from "pages/routePaths";

import styles from "./CreateConnectionTitleBlock.module.scss";

type StepStatus = "complete" | "active" | "incomplete";
const COMPLETE = "complete";
const ACTIVE = "active";
const INCOMPLETE = "incomplete";
const SOURCEID_PARAM = "sourceId";
const DESTINATIONID_PARAM = "destinationId";

interface ConnectionSteps {
  defineSource: StepStatus;
  defineDestination: StepStatus;
  configureConnection: StepStatus;
}

const calculateStepStatuses = (source: string | null, destination: string | null): ConnectionSteps | undefined => {
  if (!source && !destination) {
    return {
      defineSource: ACTIVE,
      defineDestination: INCOMPLETE,
      configureConnection: INCOMPLETE,
    };
  }

  if (source && !destination) {
    return {
      defineSource: COMPLETE,
      defineDestination: ACTIVE,
      configureConnection: INCOMPLETE,
    };
  }
  if (destination && !source) {
    return {
      defineSource: ACTIVE,
      defineDestination: COMPLETE,
      configureConnection: INCOMPLETE,
    };
  }
  if (source && destination) {
    return {
      defineSource: COMPLETE,
      defineDestination: COMPLETE,
      configureConnection: ACTIVE,
    };
  }
  return undefined;
};

const StepItem: React.FC<{ state: StepStatus; step: keyof ConnectionSteps; value: number }> = ({
  state,
  step,
  value,
}) => {
  const color = state === INCOMPLETE ? "grey" : "blue";
  const messageId =
    step === "defineSource"
      ? "connectionForm.defineSource"
      : step === "defineDestination"
      ? "connectionForm.defineDestination"
      : "connectionForm.configureConnection";

  return (
    <FlexContainer alignItems="center" gap="sm">
      <NumberBadge value={value} outline={state !== ACTIVE} color={color} />
      <Text color={color} size="lg">
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

  const stepStatuses = calculateStepStatuses(sourceId, destinationId);
  if (!stepStatuses) {
    // this should not be a possible state, but we'll handle it just in case
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}`} />;
  }

  return (
    <Box pb="lg">
      <FlexContainer direction="column" gap="xl">
        {(sourceId || destinationId) && (
          <FlexContainer alignItems="center">
            {sourceId ? <SourceBlock sourceId={sourceId} /> : <ConnectorPlaceholder />}
            <Icon type="arrowRight" />
            {destinationId ? <DestinationBlock destinationId={destinationId} /> : <ConnectorPlaceholder />}
          </FlexContainer>
        )}
        <FlexContainer gap="lg" alignItems="center">
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
