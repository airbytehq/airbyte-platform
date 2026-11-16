import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";

interface ConnectorLists<TConnector> {
  drafts: TConnector[];
  readyConnectors: TConnector[];
}

/**
 * The drafts to create and delete so that each picked definition has one connector. A pick with a ready connector
 * already has its connector, so it gets no draft. Only drafts are deleted, so a ready connector stays.
 */
export const getDraftChanges = <TConnector>(
  pickedDefinitionIds: string[],
  { drafts, readyConnectors }: ConnectorLists<TConnector>,
  getDefinitionId: (connector: TConnector) => string
) => {
  const existingDefinitionIds = new Set([...drafts, ...readyConnectors].map(getDefinitionId));
  const pickedIds = new Set(pickedDefinitionIds);

  return {
    definitionIdsToCreate: pickedDefinitionIds.filter((definitionId) => !existingDefinitionIds.has(definitionId)),
    draftsToDelete: drafts.filter((draft) => !pickedIds.has(getDefinitionId(draft))),
  };
};

/**
 * Splits the connectors into drafts and ready connectors. A passing check makes a draft ready, so a step that reads both
 * lists from one snapshot finds the connector in exactly one of them.
 */
export const splitByDraftState = <TConnector extends { isDraft?: boolean }>(
  connectors: TConnector[]
): ConnectorLists<TConnector> => ({
  drafts: connectors.filter(({ isDraft }) => isDraft),
  readyConnectors: connectors.filter(({ isDraft }) => !isDraft),
});

/**
 * The picks of a returning member: the definitions of the draft and the ready sources. A ready source counts as a pick,
 * so a source that passed its test stays picked. Each source starts as a draft, and the drafts are created in pick
 * order, so the creation time gives the pick order back. The creation time has whole seconds, so sources created in
 * the same second keep the list order, drafts first.
 */
export const getSourcePicks = (drafts: SourceRead[], readySources: SourceRead[]) => {
  const byCreation = [...drafts, ...readySources].sort((a, b) => (a.createdAt ?? 0) - (b.createdAt ?? 0));
  return [...new Set(byCreation.map(({ sourceDefinitionId }) => sourceDefinitionId))];
};

/**
 * The ready source that the source setup step opens: the first ready source of a pick. A ready source whose definition
 * is no longer picked is not opened.
 */
export const getPickedReadySource = (readySources: SourceRead[], sourcePicks: string[]) =>
  readySources.find(({ sourceDefinitionId }) => sourcePicks.includes(sourceDefinitionId));

/**
 * The destination that the destination setup step opens: the ready destination of the choice, else the draft. The flow
 * keeps organization editors out of the normal app, so a ready destination exists only after the destination setup step
 * passed the connection test. After a reload the choice comes from the draft and the ready destinations, so the choice
 * is always known, and a ready destination of another definition is not opened.
 */
export const getStepDestination = (
  readyDestinations: DestinationRead[],
  draft: DestinationRead | undefined,
  isChosen: (destination: DestinationRead) => boolean
) => readyDestinations.find(isChosen) ?? draft;
