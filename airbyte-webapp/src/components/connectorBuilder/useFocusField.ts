import { useCallback, useEffect, useState } from "react";
import { FieldValues, UseFormGetValues, useFormContext } from "react-hook-form";

import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderStreamTab } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";
import { getFirstOAuthStreamView } from "./utils";

export const DATA_FIELD_FOCUSED = "data-field-focused";

export const useFocusField = () => {
  const { setValue, getValues } = useFormContext();
  const [focusPath, setFocusPath] = useState<string | undefined>(undefined);
  const streamTab = useBuilderWatch("streamTab");

  useEffect(() => {
    if (!focusPath) {
      return;
    }

    // First try to find an input element with right field path, then fall back to any element
    let fieldToFocus =
      document.querySelector(`input[data-field-path="${focusPath}"]`) ??
      document.querySelector(`[data-field-path="${focusPath}"] textarea`) ??
      document.querySelector(`[data-field-path="${focusPath}"]`);
    if (!fieldToFocus) {
      // Check if path ends in .{number} and extract base path
      const basePathMatch = focusPath.match(/^(.+)\.\d+$/);
      if (!basePathMatch) {
        return;
      }
      // Try finding element with the base path
      const baseFieldToFocus = document.querySelector(`[data-field-path="${basePathMatch[1]}"]`);
      if (!baseFieldToFocus) {
        return;
      }
      fieldToFocus = baseFieldToFocus;
    }

    const streamTabToFocus = getStreamTabFromPath(focusPath);
    if (streamTabToFocus !== streamTab) {
      setValue("streamTab", streamTabToFocus);
    }

    fieldToFocus.scrollIntoView({ behavior: "smooth", block: "center" });
    fieldToFocus.setAttribute(DATA_FIELD_FOCUSED, "true");
    setTimeout(() => {
      if (fieldToFocus instanceof HTMLElement) {
        fieldToFocus?.focus();

        // Place cursor at the end for input and textarea elements
        if (fieldToFocus instanceof HTMLInputElement || fieldToFocus instanceof HTMLTextAreaElement) {
          const valueLength = fieldToFocus.value.length;
          fieldToFocus.setSelectionRange(valueLength, valueLength);
        }
      }
    }, 500);
    setFocusPath(undefined);
  }, [focusPath, setValue, streamTab]);

  const focusField = useCallback(
    (path: string) => {
      const view = getViewFromPath(path, getValues);
      if (view) {
        setValue("view", view);
      }

      setFocusPath(path);
    },
    [setValue, getValues]
  );

  return focusField;
};

export const removeFieldFocusedAttribute = (dataFieldPath: string) => {
  const targetField = document.querySelector(`[data-field-path="${dataFieldPath}"]`);
  if (targetField) {
    targetField.removeAttribute(DATA_FIELD_FOCUSED);
  }
};

export const getViewFromPath = (path: string, getValues: UseFormGetValues<FieldValues>): BuilderView => {
  const streamMatch = path.match(/^manifest\.streams\.(\d+)\..*$/);
  if (streamMatch) {
    return {
      type: "stream",
      index: Number(streamMatch[1]),
    };
  }

  const dynamicStreamMatch = path.match(/^manifest\.dynamic_streams\.(\d+)\..*$/);
  if (dynamicStreamMatch) {
    return {
      type: "dynamic_stream",
      index: parseInt(dynamicStreamMatch[1], 10),
    };
  }

  const advancedAuthMatch = path.match(/^manifest\.spec\.advanced_auth\..*$/);
  if (advancedAuthMatch) {
    const firstOAuthStreamView = getFirstOAuthStreamView(getValues);
    if (firstOAuthStreamView) {
      return firstOAuthStreamView;
    }
  }

  const connectionSpecificationMatch = path.match(/^manifest\.spec\.connection_specification\..*$/);
  if (connectionSpecificationMatch) {
    return {
      type: "inputs",
    };
  }

  if (path.startsWith("testingValues")) {
    return {
      type: "inputs",
    };
  }

  return {
    type: "global",
  };
};

export const getStreamTabFromPath = (path: string): BuilderStreamTab | undefined => {
  const targetField = document.querySelector(`[data-field-path="${path}"]`);
  if (!targetField) {
    return undefined;
  }

  const tabContainer = targetField.closest("[data-stream-tab]");
  if (!tabContainer) {
    return undefined;
  }

  const streamTab = tabContainer.getAttribute("data-stream-tab") as BuilderStreamTab | null;
  return streamTab ?? undefined;
};
