import {
  useCreateDestination,
  useCreateSource,
  useDeleteDestination,
  useDeleteSource,
  useDestinationDefinitionList,
  useInvalidateFirstDestinationPage,
  useInvalidateFirstSourcePage,
  useSourceDefinitionList,
} from "core/api";

import { getDraftChanges } from "./draftConnectors";
import { useDestinationLists, useSourceLists } from "./useConnectorLists";

/** A draft holds only the picked connector, so it has an empty configuration. */
const EMPTY_CONFIGURATION = {};

/** A draft is a saved onboarding answer, not a resource milestone, so its create and delete send no Segment event. */
const DRAFT_MUTATION_OPTIONS = { trackAnalytics: false };

/** Every pick comes from the definition list, so a pick with no definition is a broken contract. */
class MissingDefinitionError extends Error {
  constructor(definitionId: string) {
    super(`Connector definition ${definitionId} is not in the definition list`);
  }
}

/** The save mutations send this error to the error boundary, so the flow stops instead of skipping the pick. */
export const isMissingDefinitionError = (error: unknown) => error instanceof MissingDefinitionError;

const getDefinition = <TDefinition>(definitionMap: Map<string, TDefinition>, definitionId: string) => {
  const definition = definitionMap.get(definitionId);
  if (!definition) {
    throw new MissingDefinitionError(definitionId);
  }
  return definition;
};

/**
 * Makes the workspace's draft sources match the picks. A pick with a ready source gets no draft. Drafts are created
 * one at a time in pick order, so their creation time keeps the order to the second. The create and delete hooks show
 * their own error notification.
 */
export const useSyncDraftSources = () => {
  const sourceLists = useSourceLists();
  const { sourceDefinitionMap } = useSourceDefinitionList();
  const { mutateAsync: createSource } = useCreateSource(DRAFT_MUTATION_OPTIONS);
  const { mutateAsync: deleteSource } = useDeleteSource(DRAFT_MUTATION_OPTIONS);
  const invalidateDrafts = useInvalidateFirstSourcePage();

  return async (pickedDefinitionIds: string[]) => {
    const { definitionIdsToCreate, draftsToDelete } = getDraftChanges(
      pickedDefinitionIds,
      sourceLists,
      ({ sourceDefinitionId }) => sourceDefinitionId
    );
    const definitionsToCreate = definitionIdsToCreate.map((id) => getDefinition(sourceDefinitionMap, id));

    try {
      await Promise.all(draftsToDelete.map((source) => deleteSource({ source })));
      for (const { name, sourceDefinitionId } of definitionsToCreate) {
        await createSource({
          values: { name, connectionConfiguration: EMPTY_CONFIGURATION, createAsDraft: true },
          sourceConnector: { name, sourceDefinitionId },
        });
      }
    } finally {
      await invalidateDrafts();
    }
  };
};

/** Makes the workspace's draft destinations match the single pick. A pick with a ready destination gets no draft. */
export const useSyncDraftDestination = () => {
  const destinationLists = useDestinationLists();
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();
  const { mutateAsync: deleteDestination } = useDeleteDestination(DRAFT_MUTATION_OPTIONS);
  const invalidateDrafts = useInvalidateFirstDestinationPage();

  return async (pickedDefinitionId: string) => {
    const { definitionIdsToCreate, draftsToDelete } = getDraftChanges(
      [pickedDefinitionId],
      destinationLists,
      ({ destinationDefinitionId }) => destinationDefinitionId
    );
    const definitionsToCreate = definitionIdsToCreate.map((id) => getDefinition(destinationDefinitionMap, id));

    try {
      await Promise.all(draftsToDelete.map((destination) => deleteDestination({ destination })));
      await Promise.all(
        definitionsToCreate.map(({ name, destinationDefinitionId }) =>
          createDestination({
            values: { name, connectionConfiguration: EMPTY_CONFIGURATION, createAsDraft: true },
            destinationConnector: { name, destinationDefinitionId },
          })
        )
      );
    } finally {
      await invalidateDrafts();
    }
  };
};
