import { ConnectorIcon } from "components/common/ConnectorIcon";
import { Text } from "components/ui/Text";

import { useSourceDefinitionList, useDestinationDefinitionList } from "core/api";
import { DestinationDefinitionId, SourceDefinitionId } from "core/api/types/AirbyteClient";

import styles from "./ConnectorDefinitionBranding.module.scss";
import { FlexContainer } from "../Flex";
import { SupportLevelBadge } from "../SupportLevelBadge";

type ConnectorDefinitionBrandingProps = SourceDefinitionProps | DestinationDefinitionProps;

interface SourceDefinitionProps {
  sourceDefinitionId: SourceDefinitionId;
  destinationDefinitionId?: never;
}

interface DestinationDefinitionProps {
  destinationDefinitionId: DestinationDefinitionId;
  sourceDefinitionId?: never;
}

/**
 * Displays the branding (icon, name and release stage) for a connector definition by passing in either a sourceDefinitionId or a destinationDefinitionId
 */
export const ConnectorDefinitionBranding = ({
  sourceDefinitionId,
  destinationDefinitionId,
}: ConnectorDefinitionBrandingProps) => {
  return (
    <FlexContainer alignItems="center" gap="sm">
      {sourceDefinitionId !== undefined ? (
        <SourceDefinitionBranding sourceDefinitionId={sourceDefinitionId} />
      ) : (
        <DestinationDefinitionBranding destinationDefinitionId={destinationDefinitionId} />
      )}
    </FlexContainer>
  );
};

interface SourceDefinitionBrandingProps {
  sourceDefinitionId: SourceDefinitionId;
}

const SourceDefinitionBranding: React.FC<SourceDefinitionBrandingProps> = ({ sourceDefinitionId }) => {
  const { sourceDefinitionMap } = useSourceDefinitionList();

  const sourceDefinition = sourceDefinitionMap.get(sourceDefinitionId);

  return sourceDefinition ? (
    <>
      <ConnectorIcon icon={sourceDefinition.icon} />
      <Text className={styles.name}>{sourceDefinition.name}</Text>
      <SupportLevelBadge supportLevel={sourceDefinition.supportLevel} custom={sourceDefinition.custom} />
    </>
  ) : null;
};

interface DestinationDefinitionBrandingProps {
  destinationDefinitionId: SourceDefinitionId;
}

const DestinationDefinitionBranding: React.FC<DestinationDefinitionBrandingProps> = ({ destinationDefinitionId }) => {
  const { destinationDefinitionMap } = useDestinationDefinitionList();

  const destinationDefinition = destinationDefinitionMap.get(destinationDefinitionId);

  return destinationDefinition ? (
    <>
      <ConnectorIcon icon={destinationDefinition.icon} />
      <Text className={styles.name}>{destinationDefinition.name}</Text>
      <SupportLevelBadge supportLevel={destinationDefinition.supportLevel} custom={destinationDefinition.custom} />
    </>
  ) : null;
};
