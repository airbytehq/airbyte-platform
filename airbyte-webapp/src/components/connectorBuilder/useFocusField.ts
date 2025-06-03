import { useCallback, useEffect, useState } from "react";
import { useFormContext } from "react-hook-form";

import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderStreamTab } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

export const useFocusField = () => {
  const { setValue } = useFormContext();
  const [focusPath, setFocusPath] = useState<string | undefined>(undefined);
  // undefined means that the tab that should be focused is not yet known
  // null means that there is no tab that needs to first be focused
  const [focusTab, setFocusTab] = useState<BuilderStreamTab | null | undefined>(undefined);
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

    if (focusTab === undefined) {
      const tabContainer = fieldToFocus.closest("[data-stream-tab]");
      if (tabContainer) {
        const streamTab = tabContainer.getAttribute("data-stream-tab") as BuilderStreamTab;
        setFocusTab(streamTab);
        setValue("streamTab", streamTab);
      } else {
        setFocusTab(null);
      }
      return;
    }

    if (focusTab === null || streamTab === focusTab) {
      fieldToFocus.scrollIntoView({ behavior: "smooth", block: "center" });
      if (fieldToFocus instanceof HTMLElement) {
        fieldToFocus.focus();

        // Place cursor at the end for input and textarea elements
        if (fieldToFocus instanceof HTMLInputElement || fieldToFocus instanceof HTMLTextAreaElement) {
          const valueLength = fieldToFocus.value.length;
          fieldToFocus.setSelectionRange(valueLength, valueLength);
        }
      }
      setFocusPath(undefined);
      setFocusTab(undefined);
    }
  }, [focusPath, focusTab, setValue, streamTab]);

  const focusField = useCallback(
    (path: string) => {
      const view = getViewFromPath(path);
      if (view) {
        setValue("view", view);
      }

      setFocusPath(path);
      setFocusTab(undefined);
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

  return undefined;
};
