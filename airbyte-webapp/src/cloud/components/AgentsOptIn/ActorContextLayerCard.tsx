import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ActorContextLayerCard.module.scss";
import {
  ActorAgentAccessToggle,
  ActorSemanticSearchToggle,
  useShowActorContextLayerToggles,
} from "./ActorContextLayerToggles";

interface ActorContextLayerCardProps {
  actorId: string;
  actorType: "source" | "destination";
}

export const ActorContextLayerCard: React.FC<ActorContextLayerCardProps> = ({ actorId, actorType }) => {
  const { formatMessage } = useIntl();
  const isVisible = useShowActorContextLayerToggles();

  if (!isVisible) {
    return null;
  }

  return (
    <Card title={formatMessage({ id: "cloud.contextLayer.actorCard.title" })}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer className={styles.row} justifyContent="space-between" alignItems="center">
          <div className={styles.text}>
            <Text className={styles.title} size="sm" bold>
              <FormattedMessage id="cloud.contextLayer.actor.agentAccess" />
            </Text>
            <Text className={styles.description} size="sm" color="grey">
              <FormattedMessage id="cloud.contextLayer.actorCard.agentAccess.description" values={{ actorType }} />
            </Text>
          </div>
          <ActorAgentAccessToggle actorId={actorId} actorType={actorType} />
        </FlexContainer>
        <FlexContainer className={styles.row} justifyContent="space-between" alignItems="center">
          <div className={styles.text}>
            <Text className={styles.title} size="sm" bold>
              <FormattedMessage id="cloud.contextLayer.actor.semanticSearch" />
            </Text>
            <Text className={styles.description} size="sm" color="grey">
              <FormattedMessage id="cloud.contextLayer.actorCard.semanticSearch.description" values={{ actorType }} />
            </Text>
          </div>
          <ActorSemanticSearchToggle actorId={actorId} actorType={actorType} />
        </FlexContainer>
      </FlexContainer>
    </Card>
  );
};
