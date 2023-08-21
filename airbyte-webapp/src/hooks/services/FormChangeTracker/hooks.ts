import uniqueId from "lodash/uniqueId";
import { useCallback, useMemo, useRef } from "react";
import { flushSync } from "react-dom";
import { createGlobalState } from "react-use";

import { FormChangeTrackerServiceApi } from "./types";

export const useChangedFormsById = createGlobalState<Record<string, boolean>>({});

export const useUniqueFormId = (formId?: string) => useMemo(() => formId ?? uniqueId("form_"), [formId]);

export const useFormChangeTrackerService = (): FormChangeTrackerServiceApi => {
  const [changedFormsById, setChangedFormsById] = useChangedFormsById();
  const changedFormsByIdRef = useRef(changedFormsById);
  changedFormsByIdRef.current = changedFormsById;

  const hasFormChanges = useMemo<boolean>(
    () => Object.values(changedFormsById ?? {}).some((changed) => !!changed),
    [changedFormsById]
  );

  const clearAllFormChanges = useCallback(() => {
    // We require the flushSync to make sure React immediately will rerender this change (and thus
    // remove all potential blockers) before a navigation call in the same callback might happen.
    // If we don't do this, the navigate call might still see the blocker existing since it's in the
    // same render batch.
    flushSync(() => {
      setChangedFormsById({});
    });
  }, [setChangedFormsById]);

  const clearFormChange = useCallback(
    (id: string) => {
      flushSync(() => {
        setChangedFormsById(({ [id]: _, ...state }) => state);
      });
    },
    [setChangedFormsById]
  );

  const trackFormChange = useCallback(
    (id: string, changed: boolean) => {
      if (Boolean(changedFormsByIdRef.current?.[id]) !== changed) {
        flushSync(() => {
          setChangedFormsById((state) => ({ ...state, [id]: changed }));
        });
      }
    },
    [setChangedFormsById]
  );

  return {
    hasFormChanges,
    trackFormChange,
    clearFormChange,
    clearAllFormChanges,
  };
};
