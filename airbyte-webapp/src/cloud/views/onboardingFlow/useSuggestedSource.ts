import { useSourceDefinitionList, useSourceDefinitionSpecificationList } from "core/api";
import { SourceDefinitionSpecificationRead } from "core/api/types/AirbyteClient";
import { isDefined } from "core/utils/common";

import { getPickedReadySource } from "./draftConnectors";
import { getCertifiedDefinitionIds, getEasiestSource } from "./suggestedSource";
import { useSourceLists } from "./useConnectorLists";

const getManagedOAuthDefinitionIds = (specs: SourceDefinitionSpecificationRead[]) =>
  new Set(
    specs
      .filter((spec) => spec.advancedAuthGlobalCredentialsAvailable)
      .map(({ sourceDefinitionId }) => sourceDefinitionId)
  );

/**
 * The source pick that the flow sets up first: the pick of a ready source, which the source setup step opens, else the
 * easiest pick. Airbyte-managed OAuth is only on the spec, so this loads the spec of each certified pick. A pick whose
 * definition is not in the list cannot be set up, so it is left out.
 */
export const useSuggestedSource = (sourcePicks: string[]) => {
  const { sourceDefinitionMap } = useSourceDefinitionList();
  const { readyConnectors: readySources } = useSourceLists();
  const definitions = sourcePicks.map((definitionId) => sourceDefinitionMap.get(definitionId)).filter(isDefined);
  const specs = useSourceDefinitionSpecificationList(getCertifiedDefinitionIds(definitions));

  const readySource = getPickedReadySource(readySources, sourcePicks);
  if (readySource) {
    return sourceDefinitionMap.get(readySource.sourceDefinitionId);
  }
  return getEasiestSource(definitions, getManagedOAuthDefinitionIds(specs));
};
