import isObject from "lodash/isObject";
import { useCallback } from "react";
import { FieldErrors, useFormContext, useFormState } from "react-hook-form";

import { assertNever } from "core/utils/asserts";
import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderState, BuilderStreamTab } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";
import { useFocusField } from "./useFocusField";

const EMPTY_ERROR_REPORT: ErrorReport = {
  global: [],
  inputs: [],
  components: [],
  stream: {},
  dynamic_stream: {},
  generated_stream: {},
  unknown: [],
};

function limitErrorReportByView(errorReport: ErrorReport, limitToViews: BuilderView[]): ErrorReport {
  return limitToViews.reduce((report, view) => {
    if (view.type === "global") {
      report.global = errorReport.global;
    } else if (view.type === "inputs") {
      report.inputs = errorReport.inputs;
    } else if (view.type === "components") {
      report.components = errorReport.components;
    } else if (view.type === "stream") {
      if (errorReport.stream.hasOwnProperty(view.index)) {
        report.stream[view.index] = errorReport.stream[view.index];
      }
    } else if (view.type === "dynamic_stream") {
      if (errorReport.dynamic_stream.hasOwnProperty(view.index)) {
        report.dynamic_stream[view.index] = errorReport.dynamic_stream[view.index];
      }
    } else if (view.type === "generated_stream") {
      if (errorReport.generated_stream.hasOwnProperty(view.index)) {
        report.generated_stream[view.index] = errorReport.generated_stream[view.index];
      }
    } else {
      assertNever(view);
    }
    return report;
  }, structuredClone(EMPTY_ERROR_REPORT));
}

function getErrorPathsForView(errorReport: ErrorReport, view: BuilderView): string[] {
  if ("index" in view) {
    return errorReport[view.type][view.index] ?? [];
  }
  return errorReport[view.type];
}

function doesViewHaveErrors(errorReport: ErrorReport, view: BuilderView): boolean {
  return getErrorPathsForView(errorReport, view).length > 0;
}

function getFirstErrorViewFromReport(errorReport: ErrorReport): BuilderView | undefined {
  const reportEntries = Object.entries(errorReport);
  for (const [_viewType, entries] of reportEntries) {
    const viewType = _viewType as BuilderView["type"];
    if (viewType === "global" || viewType === "inputs" || viewType === "components") {
      if (entries.length > 0) {
        return { type: viewType };
      }
    } else if (viewType === "generated_stream") {
      if (entries.length > 0) {
        return { type: viewType, dynamicStreamName: entries[0].dynamicStreamName, index: entries[0].index };
      }
    } else if (entries.length > 0) {
      return { type: viewType, index: entries[0].index };
    }
  }
  return undefined;
}

function isViewInViewList(view: BuilderView, viewList: BuilderView[]): boolean {
  return viewList.some(
    (v) => v.type === view.type && ("index" in v && "index" in view ? v.index === view.index : true)
  );
}

export const useBuilderErrors = () => {
  const { trigger } = useFormContext<BuilderState>();
  const { errors } = useFormState<BuilderState>();
  const view = useBuilderWatch("view");
  const { setValue, getValues } = useFormContext();
  const focusField = useFocusField();

  // Returns true if the react hook form has errors, and false otherwise.
  // If limitToViews is provided, the error check is limited to only those views.
  const hasErrors = useCallback(
    (limitToViews?: BuilderView[], limitToStreamTab?: BuilderStreamTab): boolean => {
      const builderViewToErrorPaths = getBuilderViewToErrorPaths(errors);
      const viewFilteredViewToErrorPaths = limitToViews
        ? limitErrorReportByView(builderViewToErrorPaths, limitToViews)
        : builderViewToErrorPaths;

      if (limitToStreamTab) {
        return Object.values(viewFilteredViewToErrorPaths.stream).some((errorPaths) =>
          errorPaths.some((errorPath) => getStreamTabFromErrorPath(errorPath) === limitToStreamTab)
        );
      }

      const entries = Object.entries(viewFilteredViewToErrorPaths);
      for (let i = 0; i < entries.length; i++) {
        const [viewType, errors] = entries[i];
        if (viewType === "global" || viewType === "inputs" || viewType === "components") {
          if (errors.length > 0) {
            return true;
          }
        }
        // errors is a Record<number, string[]>
        const nestedErrors = Object.values(errors);
        if (nestedErrors.length > 0) {
          return true;
        }
      }

      return false;
    },
    [errors]
  );

  const getErrorPathAndView = useCallback(
    (limitToViews?: BuilderView[]): { view: BuilderView; errorPath: string } | undefined => {
      const builderViewToErrorPaths = getBuilderViewToErrorPaths(errors);

      // if already on a view with an error, scroll to the first erroring field
      if (
        (!limitToViews || isViewInViewList(view, limitToViews)) &&
        doesViewHaveErrors(builderViewToErrorPaths, view)
      ) {
        return { view, errorPath: getErrorPathsForView(builderViewToErrorPaths, view)[0] };
      }

      if (limitToViews) {
        const invalidViews = limitToViews.filter((view) => doesViewHaveErrors(builderViewToErrorPaths, view));
        if (invalidViews.length === 0) {
          return undefined;
        }
        return { view: invalidViews[0], errorPath: getErrorPathsForView(builderViewToErrorPaths, invalidViews[0])[0] };
      }

      if (!limitToViews) {
        const viewToSelect = getFirstErrorViewFromReport(builderViewToErrorPaths);
        if (viewToSelect) {
          return {
            view: viewToSelect,
            errorPath: getErrorPathsForView(builderViewToErrorPaths, viewToSelect)[0],
          };
        }
      }

      return undefined;
    },
    [view, errors]
  );

  const highlightErrorField = useCallback(
    (errorPath: string) => {
      focusField(errorPath);

      // Mark the field as touched to show error message
      setValue(errorPath, getValues(errorPath), { shouldTouch: true });
    },
    [focusField, getValues, setValue]
  );

  const validateAndTouch = useCallback(
    (callback?: () => void, limitToViews?: BuilderView[]) => {
      trigger().then((isValid) => {
        if (isValid) {
          callback?.();
          return;
        }

        const errorPathAndView = getErrorPathAndView(limitToViews);
        if (errorPathAndView) {
          highlightErrorField(errorPathAndView.errorPath);
          return;
        }

        callback?.();
      });
    },
    [trigger, getErrorPathAndView, highlightErrorField]
  );

  const getErrorPaths = useCallback(
    (view: BuilderView) => {
      return getErrorPathsForView(getBuilderViewToErrorPaths(errors), view);
    },
    [errors]
  );

  return { hasErrors, validateAndTouch, getErrorPaths };
};

const getStreamTabFromErrorPath = (errorPath: string): BuilderStreamTab | undefined => {
  const field = document.querySelector(`[data-field-path="${errorPath}"]`);
  if (field) {
    const tabContainer = field.closest("[data-stream-tab]");
    if (tabContainer) {
      return tabContainer.getAttribute("data-stream-tab") as BuilderStreamTab;
    }
  }

  return undefined;
};

interface ErrorReport {
  global: string[];
  inputs: string[];
  components: string[];
  stream: Record<number, string[]>;
  dynamic_stream: Record<number, string[]>;
  generated_stream: Record<number, string[]>;
  unknown: string[];
}

const getBuilderViewToErrorPaths = (errors: FieldErrors<BuilderState>): ErrorReport => {
  const result: ErrorReport = structuredClone(EMPTY_ERROR_REPORT);

  const isRecord = (value: unknown): value is Record<string, unknown> => {
    return !!value && isObject(value);
  };

  const isError = (value: unknown): boolean => {
    return isRecord(value) && "message" in value && "ref" in value && "type" in value;
  };

  function processErrors(obj: Record<string, unknown>, currentPath: string[] = []): void {
    for (const key in obj) {
      const value = obj[key];

      if (isError(value)) {
        const view: BuilderView | { type: "unknown" } =
          currentPath[0] === "testingValues"
            ? { type: "inputs" }
            : currentPath[0] === "manifest"
            ? currentPath[1] === "streams"
              ? { type: "stream", index: Number(currentPath[2]) }
              : currentPath[1] === "dynamic_streams"
              ? { type: "dynamic_stream", index: Number(currentPath[2]) }
              : currentPath[1] === "spec"
              ? { type: "inputs" }
              : { type: "global" }
            : { type: "unknown" };
        const fullPath = [...currentPath, key].join(".");
        if (typeof view === "object" && "index" in view) {
          if (!result[view.type][view.index]) {
            result[view.type][view.index] = [];
          }
          result[view.type][view.index].push(fullPath);
        } else {
          if (!result[view.type]) {
            result[view.type] = [];
          }
          result[view.type].push(fullPath);
        }
      } else if (Array.isArray(value)) {
        // If the value is an array, process its elements
        value.forEach((item, index) => {
          if (item && typeof item === "object") {
            processErrors(item, [...currentPath, key, index.toString()]);
          }
        });
      } else if (isRecord(value)) {
        // If the value is a nested object, recurse into it
        processErrors(value, [...currentPath, key]);
      }
    }
  }

  processErrors(errors);

  return result;
};
