import { SourceRead } from "core/api/types/AirbyteClient";

/** The drafts to create and delete so that the drafts match the picks, one draft per picked definition. */
export const getDraftChanges = <TDraft>(
  pickedDefinitionIds: string[],
  drafts: TDraft[],
  getDefinitionId: (draft: TDraft) => string
) => {
  const draftDefinitionIds = new Set(drafts.map(getDefinitionId));
  const pickedIds = new Set(pickedDefinitionIds);

  return {
    definitionIdsToCreate: pickedDefinitionIds.filter((definitionId) => !draftDefinitionIds.has(definitionId)),
    draftsToDelete: drafts.filter((draft) => !pickedIds.has(getDefinitionId(draft))),
  };
};

/**
 * The drafts are created in pick order, so their creation time gives the pick order back. The creation time has whole
 * seconds, so drafts created in the same second keep the list order, which is by name.
 */
export const getSourcePicksFromDrafts = (drafts: SourceRead[]) => {
  const byCreation = [...drafts].sort((a, b) => (a.createdAt ?? 0) - (b.createdAt ?? 0));
  return [...new Set(byCreation.map(({ sourceDefinitionId }) => sourceDefinitionId))];
};
