import { diff, Diff, revertChange, applyChange } from "deep-diff";
import cloneDeep from "lodash/cloneDeep";
import forEachRight from "lodash/forEachRight";
import isEqual from "lodash/isEqual";
import { useCallback, useLayoutEffect, useMemo, useReducer, useRef } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { useConnectorBuilderFormManagementState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { BuilderFormValues } from "./types";

export interface UndoRedo {
  canUndo: boolean;
  canRedo: boolean;
  undo: () => void;
  redo: () => void;
  registerChange: (newFormValues: BuilderFormValues) => void;
  clearHistory: () => void;
}

export const useUndoRedo = (): UndoRedo => {
  const [state, dispatch] = useReducer(diffReducer, initialState);
  const { storedFormValues, diffHistory, diffHistoryIndex, modifiedPath } = state;

  const registerChange = useCallback((newFormValues: BuilderFormValues) => {
    dispatch({ type: "REGISTER_CHANGE", newFormValues });
  }, []);

  const { setValue, trigger } = useFormContext();

  const canUndo = useMemo(
    () => diffHistoryIndex >= 0 && diffHistoryIndex < diffHistory.length,
    [diffHistory.length, diffHistoryIndex]
  );
  const undo = useCallback(() => {
    if (!canUndo || !storedFormValues) {
      return;
    }
    dispatch({ type: "UNDO" });
  }, [canUndo, storedFormValues]);

  const canRedo = useMemo(
    () => diffHistoryIndex < diffHistory.length - 1 && diffHistoryIndex >= -1,
    [diffHistory.length, diffHistoryIndex]
  );
  const redo = useCallback(() => {
    if (!canRedo || !storedFormValues) {
      return;
    }
    dispatch({ type: "REDO" });
  }, [canRedo, storedFormValues]);

  const clearHistory = useCallback(() => {
    dispatch({ type: "CLEAR_HISTORY" });
  }, []);

  const { setScrollToField } = useConnectorBuilderFormManagementState();
  // Update the actual form values when storedFormValues changes, to apply undos and redos.
  // Use a ref for formValues comparison so that effect is only triggered when storedFormValues
  // changes, and formValues are only set if there is a difference to avoid infinite loops.
  const builderState = useWatch();
  const builderStateRef = useRef(builderState);
  builderStateRef.current = builderState;
  // There is a potential race condition here which useLayoutEffect helps to avoid:
  // If the user performs consecutive undos/redos with a delay between them that is close to the debounce
  // time for triggering registerChange from a formValues update in Builder.tsx, then the first undo/redo
  // could trigger a registerChange call in between when the second undo/redo action updates storedFormValues,
  // and when this effect hook runs.
  // This causes the registerChange to compare the updated form values from the first undo/redo with the
  // storedFormValues from the second undo/redo, which results in an inaccurate diff being computed, which
  // wipes out the future redo history.
  // Using useLayoutEffect here instead of useEffect makes the time between the second undo/redo updating
  // storedFormValues and this effect hook running so small that it is virtually impossible for the user to
  // run into this scenario. It could not be reproduced while testing.
  useLayoutEffect(() => {
    if (storedFormValues && !isEqual(storedFormValues, builderStateRef.current.formValues)) {
      setValue("formValues", storedFormValues);
      // Must manually trigger validation after setting form values, because setValue doesn't trigger
      // validation on fields that are set directly to undefined even if shouldValidate is set to true.
      trigger();

      if (modifiedPath) {
        setScrollToField(modifiedPath);
      }
      if (modifiedPath === "formValues.streams") {
        // stream was removed, switch view to last stream if view is currently set to the deleted stream
        if (
          storedFormValues.streams.length < builderStateRef.current.formValues.streams.length &&
          builderStateRef.current.view >= storedFormValues.streams.length
        ) {
          setValue("view", { type: "stream", index: storedFormValues.streams.length - 1 });
        }

        // stream was added, switch view to last stream
        if (storedFormValues.streams.length > builderStateRef.current.formValues.streams.length) {
          setValue("view", { type: "stream", index: storedFormValues.streams.length - 1 });
        }
      } else if (modifiedPath?.startsWith("formValues.streams.")) {
        const streamPathRegex = /^formValues\.streams\.(\d+)\..*$/;
        const match = modifiedPath.match(streamPathRegex);
        if (match) {
          setValue("view", { type: "stream", index: Number(match[1]) });
        }
      } else if (modifiedPath?.startsWith("formValues.inputs")) {
        setValue("view", { type: "inputs" });
      } else if (modifiedPath?.startsWith("formValues.global")) {
        setValue("view", { type: "global" });
      }
    }
  }, [modifiedPath, setScrollToField, setValue, storedFormValues, trigger]);

  return {
    canUndo,
    canRedo,
    undo,
    redo,
    registerChange,
    clearHistory,
  };
};

interface DiffState {
  storedFormValues: BuilderFormValues | undefined;
  diffHistory: Array<Array<Diff<BuilderFormValues>>>;
  diffHistoryIndex: number;
  modifiedPath: string | undefined;
}

const initialState: DiffState = {
  storedFormValues: undefined,
  diffHistory: [],
  diffHistoryIndex: -1,
  modifiedPath: undefined,
};

type DiffAction =
  | { type: "REGISTER_CHANGE"; newFormValues: BuilderFormValues }
  | { type: "UNDO" }
  | { type: "REDO" }
  | { type: "CLEAR_HISTORY" };

const DIFF_HISTORY_LIMIT = 200;

function diffReducer(state: DiffState, action: DiffAction) {
  const { storedFormValues, diffHistory, diffHistoryIndex } = state;

  switch (action.type) {
    case "REGISTER_CHANGE":
      const { newFormValues } = action;
      if (storedFormValues === undefined) {
        return {
          ...state,
          storedFormValues: newFormValues,
        };
      }

      const diffs = sortArrayDiffs(diff(storedFormValues, newFormValues));
      if (diffs === undefined) {
        return state;
      }

      const newState = {
        ...state,
        storedFormValues: newFormValues,
        modifiedPath: undefined,
      };

      if (diffHistory.length === DIFF_HISTORY_LIMIT && diffHistoryIndex === diffHistory.length - 1) {
        return {
          ...newState,
          // remove the oldest history item when the limit is reached
          diffHistory: [...diffHistory.slice(1), diffs],
        };
      }
      return {
        ...newState,
        // clear any future history when a new change is made
        diffHistory: [...diffHistory.slice(0, diffHistoryIndex + 1), diffs],
        diffHistoryIndex: diffHistoryIndex + 1,
      };

    case "UNDO":
      if (!storedFormValues) {
        return state;
      }
      const revertedFormValues = cloneDeep(storedFormValues);
      // revert diffs in reverse order to properly undo changes
      forEachRight(diffHistory[diffHistoryIndex], (diff) => revertChange(revertedFormValues, revertedFormValues, diff));
      return {
        ...state,
        storedFormValues: revertedFormValues,
        diffHistoryIndex: diffHistoryIndex - 1,
        modifiedPath: diffPathToFormPath(diffHistory[diffHistoryIndex][0]?.path),
      };

    case "REDO":
      if (!storedFormValues) {
        return state;
      }
      const updatedFormValues = cloneDeep(storedFormValues);
      diffHistory[diffHistoryIndex + 1].forEach((diff) => applyChange(updatedFormValues, updatedFormValues, diff));
      return {
        ...state,
        storedFormValues: updatedFormValues,
        diffHistoryIndex: diffHistoryIndex + 1,
        modifiedPath: diffPathToFormPath(diffHistory[diffHistoryIndex + 1][0]?.path),
      };

    case "CLEAR_HISTORY":
      return {
        storedFormValues: undefined,
        diffHistory: [],
        diffHistoryIndex: -1,
        modifiedPath: undefined,
      };

    default:
      return state;
  }
}

function diffPathToFormPath(diffPath: Diff<BuilderFormValues>["path"]): string | undefined {
  if (diffPath === undefined) {
    return undefined;
  }
  return `formValues.${diffPath.join(".")}`;
}

// The deep-diff library doesn't always return diffs in the correct order for array diffs,
// so this function sorts the diffs by index to ensure that they are applied in the correct order
// when performing undos and redos.
function sortArrayDiffs(diffs: Array<Diff<BuilderFormValues>> | undefined) {
  if (diffs === undefined) {
    return undefined;
  }

  return diffs.sort((a, b) => {
    if (a.kind === "A" && b.kind === "A") {
      return a.index - b.index;
    }
    return 0;
  });
}
