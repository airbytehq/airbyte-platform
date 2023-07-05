import { Fragment } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useSearchParams } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { NumberBadge } from "components/ui/NumberBadge";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { DestinationRead, ReleaseStage, SourceRead } from "core/request/AirbyteClient";
import { useGetDestination } from "hooks/services/useDestinationHook";
import { useGetSource } from "hooks/services/useSourceHook";
import { RoutePaths } from "pages/routePaths";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";

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

const calculateStepStatuses = (source?: SourceRead, destination?: DestinationRead): ConnectionSteps | undefined => {
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

// todo: these can pull from params once that PR is here
const SourceBlock: React.FC<{ source?: SourceRead }> = ({ source }) => {
  const sourceDefinition = useSourceDefinition(source?.sourceDefinitionId);

  return <ConnectorPlaceholder icon={source?.icon} name={source?.name} stage={sourceDefinition?.releaseStage} />;
};

const DestinationBlock: React.FC<{ destination?: DestinationRead }> = ({ destination }) => {
  const destinationDefinition = useDestinationDefinition(destination?.destinationDefinitionId);

  return (
    <ConnectorPlaceholder
      icon={destination?.icon}
      name={destination?.name}
      stage={destinationDefinition?.releaseStage}
    />
  );
};

const ConnectorPlaceholder: React.FC<{ icon?: string; name?: string; stage?: ReleaseStage }> = ({
  icon,
  name,
  stage,
}) => {
  return (
    <FlexContainer alignItems="center" gap="sm">
      {icon ? <ConnectorIcon icon={icon} /> : <div className={styles.iconPlaceholder} />}
      {name ? <Text>{name}</Text> : <div className={styles.namePlaceholder} />}
      {stage && <ReleaseStageBadge stage={stage} />}
    </FlexContainer>
  );
};

export const CreateConnectionTitleBlock: React.FC = () => {
  const [searchParams] = useSearchParams();
  const { workspaceId } = useCurrentWorkspace();

  const sourceId = searchParams.get(SOURCEID_PARAM);
  const destinationId = searchParams.get(DESTINATIONID_PARAM);

  const source = useGetSource(sourceId);
  const destination = useGetDestination(destinationId);
  const stepStatuses = calculateStepStatuses(source, destination);
  if (!stepStatuses) {
    // this should not be a possible state, but we'll handle it just in case
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}`} />;
  }

  return (
    <Box pb="lg">
      <FlexContainer direction="column" gap="xl">
        {(source || destination) && (
          <FlexContainer alignItems="center">
            <SourceBlock source={source} />
            <Icon type="arrowRight" />
            <DestinationBlock destination={destination} />
          </FlexContainer>
        )}
        <FlexContainer gap="lg" alignItems="center">
          {(Object.keys(stepStatuses) as Array<keyof ConnectionSteps>).map((step, idx) => {
            return (
              <Fragment key={step}>
                <StepItem state={stepStatuses[step]} step={step} value={idx + 1} />
                {idx !== Object.keys(stepStatuses).length - 1 && <Icon type="chevronRight" size="lg" />}
              </Fragment>
            );
          })}
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
