import classNames from "classnames";
import React from "react";
import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useExperiment } from "core/services/Experiment";

import styles from "./ActorContextLayerCard.module.scss";
import {
  ActorAgentAccessToggle,
  ActorSemanticSearchToggle,
  useShowActorContextLayerToggles,
} from "./ActorContextLayerToggles";
import { ContextLayerSettingLabel } from "./ContextLayerSettingLabel";

interface ActorContextLayerCardProps {
  actorId: string;
  actorType: "source" | "destination";
}

export const ActorContextLayerCard: React.FC<ActorContextLayerCardProps> = ({ actorId, actorType }) => {
  const { formatMessage } = useIntl();
  const isVisible = useShowActorContextLayerToggles();
  const showSemanticSearch = useExperiment("platform.fusion-semantic-search-ui");

  if (!isVisible) {
    return null;
  }

  return (
    <Card title={formatMessage({ id: "cloud.contextLayer.actorCard.title" })}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer className={styles.row} justifyContent="space-between" alignItems="center">
          <ContextLayerSettingLabel setting="agentAccess" actorType={actorType} />
          <ActorAgentAccessToggle actorId={actorId} actorType={actorType} />
        </FlexContainer>
        {actorType === "source" && showSemanticSearch && (
          <FlexContainer
            className={classNames(styles.row, styles.tierTwo)}
            justifyContent="space-between"
            alignItems="center"
          >
            <ContextLayerSettingLabel setting="semanticSearch" actorType={actorType} />
            <ActorSemanticSearchToggle actorId={actorId} actorType={actorType} />
          </FlexContainer>
        )}
      </FlexContainer>
    </Card>
  );
};
