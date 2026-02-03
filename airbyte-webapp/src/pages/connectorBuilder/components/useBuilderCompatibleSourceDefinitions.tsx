import { useMemo } from "react";

import { BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE } from "area/connectorBuilder/components/constants";
import { useSourceDefinitionList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

export const useBuilderCompatibleSourceDefinitions = () => {
  const { sourceDefinitions, sourceDefinitionMap } = useSourceDefinitionList();
  const builderCompatibleSourceDefinitions = useMemo(
    () => sourceDefinitions.filter((sourceDefinition) => isBuilderCompatible(sourceDefinition)),
    [sourceDefinitions]
  );

  return { builderCompatibleSourceDefinitions, sourceDefinitionMap };
};

export const isBuilderCompatible = (sourceDefinition: SourceDefinitionRead) =>
  sourceDefinition.language === BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE;
