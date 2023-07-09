import * as LDClient from "launchdarkly-js-client-sdk";

import { ContextKind } from "hooks/services/Experiment";

import { createMultiContext, getSingleContextsFromMulti, isMultiContext } from "./contexts";

export interface LDContextState {
  context: LDClient.LDMultiKindContext | LDClient.LDSingleKindContext;
}

export type LDContextReducerAction =
  | {
      type: "add";
      context: LDClient.LDSingleKindContext;
    }
  // the user context always exists (at worst we have an anonymous user) so it doesn't make sense to remove this context
  | { type: "remove"; kind: Exclude<ContextKind, "user"> };

export const DEFAULT_USER_LOCALE = "en";

// Keeps track of the launch darkly contexts that are currently active
export const contextReducer = (state: LDContextState, action: LDContextReducerAction) => {
  const existingContexts = isMultiContext(state.context) ? getSingleContextsFromMulti(state.context) : [state.context];

  switch (action.type) {
    case "add":
      // Don't change any state if the same context already exists
      if (existingContexts.find((c) => c.kind === action.context.kind)?.key === action.context.key) {
        return state;
      }
      const newContexts = createMultiContext(
        ...existingContexts.filter((c) => c.kind !== action.context.kind),
        action.context
      );
      return { context: newContexts };
    case "remove":
      const filteredContexts = createMultiContext(...existingContexts.filter((c) => c.kind !== action.kind));
      return { context: filteredContexts };
  }
};
