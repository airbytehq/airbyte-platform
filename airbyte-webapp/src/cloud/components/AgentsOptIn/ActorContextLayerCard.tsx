import React from "react";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useAgentsSupportedSourceDefinitionIds, useAgentsSupportedDestinationDefinitionIds } from "core/api";

import styles from "./ActorContextLayerCard.module.scss";
import { ActorAgentAccessToggle, useShowActorContextLayerToggles } from "./ActorContextLayerToggles";
import { ContextLayerSettingLabel } from "./ContextLayerSettingLabel";

interface ActorContextLayerCardProps {
  actorId: string;
  actorDefinitionId: string;
  actorType: "source" | "destination";
}

export const ActorContextLayerCard: React.FC<ActorContextLayerCardProps> = ({
  actorId,
  actorDefinitionId,
  actorType,
}) => {
  const isVisible = useShowActorContextLayerToggles();
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();
  const supportedDefinitionIds =
    actorType === "source" ? supportedSourceDefinitionIds : supportedDestinationDefinitionIds;

  if (!isVisible || !supportedDefinitionIds.has(actorDefinitionId)) {
    return null;
  }

  return (
    <Card>
      <FlexContainer className={styles.row} justifyContent="space-between" alignItems="flex-start">
        <ContextLayerSettingLabel setting="agentAccess" actorType={actorType} variant="setup" />
        <ActorAgentAccessToggle actorId={actorId} actorType={actorType} />
      </FlexContainer>
    </Card>
  );
};
