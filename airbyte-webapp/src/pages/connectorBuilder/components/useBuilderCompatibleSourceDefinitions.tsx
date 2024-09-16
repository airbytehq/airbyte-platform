import { useMemo } from "react";

import { useSourceDefinitionList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

import { BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE } from "../../../components/connectorBuilder/types";

export const useBuilderCompatibleSourceDefinitions = () => {
  const isContributeEditsEnabled = useExperiment("connectorBuilder.contributeEditsToMarketplace", false);
  const { sourceDefinitions, sourceDefinitionMap } = useSourceDefinitionList();
  const builderCompatibleSourceDefinitions = useMemo(
    () =>
      isContributeEditsEnabled
        ? sourceDefinitions.filter((sourceDefinition) => isBuilderCompatible(sourceDefinition))
        : [],
    [isContributeEditsEnabled, sourceDefinitions]
  );

  return { builderCompatibleSourceDefinitions, sourceDefinitionMap };
};

export const isBuilderCompatible = (sourceDefinition: SourceDefinitionRead) =>
  sourceDefinition.language === BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE;
