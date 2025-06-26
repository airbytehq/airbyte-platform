import { useMemo } from "react";

import { useSourceDefinitionList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

import { BUILDER_COMPATIBLE_CONNECTOR_LANGUAGE } from "../../../components/connectorBuilder__deprecated/types";

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
