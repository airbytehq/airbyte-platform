import { useFirstDestinationPage, useFirstSourcePage } from "core/api";

import { splitByDraftState } from "./draftConnectors";

/** The draft and ready sources of the workspace, from the first page of its sources. */
export const useSourceLists = () => splitByDraftState(useFirstSourcePage());

/** The draft and ready destinations of the workspace, from the first page of its destinations. */
export const useDestinationLists = () => splitByDraftState(useFirstDestinationPage());
