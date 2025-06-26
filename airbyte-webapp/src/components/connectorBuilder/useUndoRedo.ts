import { diff, Diff, revertChange, applyChange } from "deep-diff";
import cloneDeep from "lodash/cloneDeep";
import forEachRight from "lodash/forEachRight";
import isEqual from "lodash/isEqual";
import { useCallback, useEffect, useMemo, useReducer, useRef } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { ConnectorManifest } from "core/api/types/ConnectorManifest";

import { useFocusField } from "./useFocusField";

export interface UndoRedo {
  canUndo: boolean;
  canRedo: boolean;
  undo: () => void;
  redo: () => void;
  registerChange: (newManifest: ConnectorManifest) => void;
  clearHistory: () => void;
}

export const useUndoRedo = (): UndoRedo => {
  const [state, dispatch] = useReducer(diffReducer, initialState);
  const { storedManifest, diffHistory, diffHistoryIndex, modifiedPath } = state;
  const focusField = useFocusField();

  const registerChange = useCallback((newManifest: ConnectorManifest) => {
    dispatch({ type: "REGISTER_CHANGE", newManifest });
  }, []);

  const { setValue, trigger } = useFormContext();

  const canUndo = useMemo(
    () => diffHistoryIndex >= 0 && diffHistoryIndex < diffHistory.length,
    [diffHistory.length, diffHistoryIndex]
  );
  const undo = useCallback(() => {
    if (!canUndo || !storedManifest) {
      return;
    }
    dispatch({ type: "UNDO" });
  }, [canUndo, storedManifest]);

  const canRedo = useMemo(
    () => diffHistoryIndex < diffHistory.length - 1 && diffHistoryIndex >= -1,
    [diffHistory.length, diffHistoryIndex]
  );
  const redo = useCallback(() => {
    if (!canRedo || !storedManifest) {
      return;
    }
    dispatch({ type: "REDO" });
  }, [canRedo, storedManifest]);

  const clearHistory = useCallback(() => {
    dispatch({ type: "CLEAR_HISTORY" });
  }, []);

  // Update the actual manifest when there is a modifiedPath to update, to apply undos and redos.
  // Use a ref for manifest comparison so that effect is only triggered when storedManifest
  // changes, and manifest are only set if there is a difference to avoid infinite loops.
  const builderState = useWatch();
  const builderStateRef = useRef(builderState);
  builderStateRef.current = builderState;
  useEffect(() => {
    if (!modifiedPath) {
      return;
    }
    if (storedManifest && !isEqual(storedManifest, builderStateRef.current.manifest)) {
      setValue("manifest", storedManifest);
      // Must manually trigger validation after setting manifest, because setValue doesn't trigger
      // validation on fields that are set directly to undefined even if shouldValidate is set to true.
      trigger();

      if (modifiedPath === "manifest.streams") {
        // stream was removed, switch view to last stream if view is currently set to the deleted stream
        if (
          storedManifest.streams &&
          builderStateRef.current.manifest.streams &&
          storedManifest.streams.length < builderStateRef.current.manifest.streams.length &&
          builderStateRef.current.view.type === "stream" &&
          builderStateRef.current.view.index >= storedManifest.streams.length
        ) {
          if (storedManifest.streams.length > 0) {
            setValue("view", { type: "stream", index: storedManifest.streams?.length - 1 });
          } else {
            setValue("view", { type: "global" });
          }
        }

        // stream was added, switch view to last stream
        if (
          storedManifest.streams &&
          builderStateRef.current.manifest.streams &&
          storedManifest.streams.length > builderStateRef.current.manifest.streams.length
        ) {
          // Find the newly added stream by comparing with current manifest
          const newStreamIndex = storedManifest.streams.findIndex(
            (stream, i) =>
              !builderStateRef.current.manifest.streams[i] ||
              !isEqual(stream, builderStateRef.current.manifest.streams[i])
          );
          if (newStreamIndex !== -1) {
            setValue("view", { type: "stream", index: newStreamIndex });
          } else {
            setValue("view", { type: "stream", index: storedManifest.streams.length - 1 });
          }
        }
      } else if (modifiedPath === "manifest.dynamic_streams") {
        // dynamic stream was removed, switch view to last dynamic stream if view is currently set to the deleted dynamic stream
        if (
          storedManifest.dynamic_streams &&
          builderStateRef.current.manifest.dynamic_streams &&
          storedManifest.dynamic_streams.length < builderStateRef.current.manifest.dynamic_streams.length &&
          builderStateRef.current.view.type === "dynamic_stream" &&
          builderStateRef.current.view.index >= storedManifest.dynamic_streams.length
        ) {
          if (storedManifest.dynamic_streams.length > 0) {
            setValue("view", { type: "dynamic_stream", index: storedManifest.dynamic_streams?.length - 1 });
          } else {
            setValue("view", { type: "global" });
          }
        }

        // dynamic stream was added, switch view to last dynamic stream
        if (
          storedManifest.dynamic_streams &&
          builderStateRef.current.manifest.dynamic_streams &&
          storedManifest.dynamic_streams.length > builderStateRef.current.manifest.dynamic_streams.length
        ) {
          // Find the newly added dynamic stream by comparing with current manifest
          const newDynamicStreamIndex = storedManifest.dynamic_streams.findIndex(
            (stream, i) =>
              !builderStateRef.current.manifest.dynamic_streams[i] ||
              !isEqual(stream, builderStateRef.current.manifest.dynamic_streams[i])
          );
          if (newDynamicStreamIndex !== -1) {
            setValue("view", { type: "dynamic_stream", index: newDynamicStreamIndex });
          } else {
            setValue("view", { type: "dynamic_stream", index: storedManifest.dynamic_streams.length - 1 });
          }
        }
      }

      focusField(modifiedPath);
      dispatch({ type: "MARK_UNDO_REDO_COMPLETED" });
    }
  }, [focusField, modifiedPath, setValue, storedManifest, trigger]);

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
  storedManifest: ConnectorManifest | undefined;
  diffHistory: Array<Array<Diff<ConnectorManifest>>>;
  diffHistoryIndex: number;
  modifiedPath: string | undefined;
}

const initialState: DiffState = {
  storedManifest: undefined,
  diffHistory: [],
  diffHistoryIndex: -1,
  modifiedPath: undefined,
};

type DiffAction =
  | { type: "REGISTER_CHANGE"; newManifest: ConnectorManifest }
  | { type: "UNDO" }
  | { type: "REDO" }
  | { type: "CLEAR_HISTORY" }
  | { type: "MARK_UNDO_REDO_COMPLETED" };

const DIFF_HISTORY_LIMIT = 200;

function diffReducer(state: DiffState, action: DiffAction) {
  const { storedManifest, diffHistory, diffHistoryIndex } = state;

  switch (action.type) {
    case "REGISTER_CHANGE":
      const { newManifest } = action;
      if (storedManifest === undefined) {
        return {
          ...state,
          storedManifest: cloneDeep(newManifest),
        };
      }

      const diffs = sortArrayDiffs(diff(storedManifest, newManifest))?.filter((diff) => {
        // ignore metadata diffs, since the user doesn't interact with the metadata
        if (diff.path && diff.path.length >= 1 && diff.path[0] === "metadata") {
          return false;
        }
        // Ignore diffs where a field is added or removed and the value is empty, since this
        // happens as a quirk of react-hook-form but is not a change that must be able to be undone
        if (
          (diff.kind === "N" &&
            (diff.rhs === undefined ||
              (diff.rhs as unknown as string) === "" ||
              (Array.isArray(diff.rhs) && diff.rhs.length === 0))) ||
          (diff.kind === "D" &&
            (diff.lhs === undefined ||
              (diff.lhs as unknown as string) === "" ||
              (Array.isArray(diff.lhs) && diff.lhs.length === 0)))
        ) {
          return false;
        }
        return true;
      });
      if (diffs === undefined || diffs.length === 0) {
        return state;
      }

      const newState = {
        ...state,
        storedManifest: cloneDeep(newManifest),
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
      if (!storedManifest) {
        return state;
      }
      const revertedManifest = cloneDeep(storedManifest);
      // revert diffs in reverse order to properly undo changes
      forEachRight(diffHistory[diffHistoryIndex], (diff) => revertChange(revertedManifest, revertedManifest, diff));
      return {
        ...state,
        storedManifest: revertedManifest,
        diffHistoryIndex: diffHistoryIndex - 1,
        modifiedPath: diffPathToFormPath(diffHistory[diffHistoryIndex][0]?.path),
      };

    case "REDO":
      if (!storedManifest) {
        return state;
      }
      const updatedManifest = cloneDeep(storedManifest);
      diffHistory[diffHistoryIndex + 1].forEach((diff) => applyChange(updatedManifest, updatedManifest, diff));
      return {
        ...state,
        storedManifest: updatedManifest,
        diffHistoryIndex: diffHistoryIndex + 1,
        modifiedPath: diffPathToFormPath(diffHistory[diffHistoryIndex + 1][0]?.path),
      };

    case "CLEAR_HISTORY":
      return {
        storedManifest: undefined,
        diffHistory: [],
        diffHistoryIndex: -1,
        modifiedPath: undefined,
      };

    case "MARK_UNDO_REDO_COMPLETED":
      return {
        ...state,
        modifiedPath: undefined,
      };

    default:
      return state;
  }
}

function diffPathToFormPath(diffPath: Diff<ConnectorManifest>["path"]): string | undefined {
  if (diffPath === undefined) {
    return undefined;
  }
  return `manifest.${diffPath.join(".")}`;
}

// The deep-diff library doesn't always return diffs in the correct order for array diffs,
// so this function sorts the diffs by index to ensure that they are applied in the correct order
// when performing undos and redos.
function sortArrayDiffs(diffs: Array<Diff<ConnectorManifest>> | undefined) {
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
