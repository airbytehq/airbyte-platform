import { useCallback, useEffect, useState } from "react";
import { useFormContext } from "react-hook-form";

import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderStreamTab } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

export const useFocusField = () => {
  const { setValue } = useFormContext();
  const [focusPath, setFocusPath] = useState<string | undefined>(undefined);
  const streamTab = useBuilderWatch("streamTab");

  useEffect(() => {
    if (!focusPath) {
      return;
    }

    // First try to find an input element with right field path, then fall back to any element
    let fieldToFocus = document.querySelector(`input[data-field-path="${focusPath}"]`);
    if (!fieldToFocus) {
      fieldToFocus = document.querySelector(`[data-field-path="${focusPath}"]`);
    }
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

    const tabContainer = fieldToFocus.closest("[data-stream-tab]");
    if (tabContainer) {
      const streamTabToFocus = tabContainer.getAttribute("data-stream-tab") as BuilderStreamTab;
      if (streamTabToFocus !== streamTab) {
        setValue("streamTab", streamTabToFocus);
      }
    }

    fieldToFocus.scrollIntoView({ behavior: "smooth", block: "center" });
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
      const view = getViewFromPath(path);
      if (view) {
        setValue("view", view);
      }

      setFocusPath(path);
    },
    [setValue]
  );

  return focusField;
};

const getViewFromPath = (path: string): BuilderView | undefined => {
  const streamMatch = path.match(/^manifest.streams\.(\d+)\..*$/);
  if (streamMatch) {
    return {
      type: "stream",
      index: Number(streamMatch[1]),
    };
  }

  const dynamicStreamMatch = path.match(/^manifest.dynamic_streams\.(\d+)\..*$/);
  if (dynamicStreamMatch) {
    return {
      type: "dynamic_stream",
      index: parseInt(dynamicStreamMatch[1], 10),
    };
  }

  const specMatch = path.match(/^manifest.spec\..*$/);
  if (specMatch) {
    return {
      type: "inputs",
    };
  }

  return {
    type: "global",
  };
};
