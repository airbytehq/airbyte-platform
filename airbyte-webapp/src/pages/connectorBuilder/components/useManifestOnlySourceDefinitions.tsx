import { useMemo } from "react";

import { useSourceDefinitionList } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

export const useManifestOnlySourceDefinitions = () => {
  const isContributeEditsEnabled = useExperiment("connectorBuilder.contributeEditsToMarketplace", false);
  const { sourceDefinitions, sourceDefinitionMap } = useSourceDefinitionList();
  const manifestOnlySourceDefinitions = useMemo(
    () =>
      isContributeEditsEnabled
        ? sourceDefinitions.filter((sourceDefinition) => sourceDefinition.language === "manifest-only")
        : [],
    [isContributeEditsEnabled, sourceDefinitions]
  );

  return { manifestOnlySourceDefinitions, sourceDefinitionMap };
};
