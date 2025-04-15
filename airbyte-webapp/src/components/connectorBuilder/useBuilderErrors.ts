import isObject from "lodash/isObject";
import isString from "lodash/isString";
import { useCallback, useRef } from "react";
import { FieldErrors, useFormContext, useFormState } from "react-hook-form";

import {
  BuilderView,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import {
  BuilderState,
  BuilderStreamTab,
  DeclarativeOAuthAuthenticatorType,
  extractInterpolatedConfigKey,
} from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

export const useBuilderErrors = () => {
  const { trigger } = useFormContext<BuilderState>();
  const { errors } = useFormState<BuilderState>();
  const errorsRef = useRef(errors);
  errorsRef.current = errors;
  const view = useBuilderWatch("view");
  const formValues = useBuilderWatch("formValues");

  const { setScrollToField } = useConnectorBuilderFormManagementState();
  const { setValue, getValues } = useFormContext();

  // Returns true if the react hook form has errors, and false otherwise.
  // If limitToViews is provided, the error check is limited to only those views.
  const hasErrors = useCallback((limitToViews?: BuilderView[], limitToStreamTab?: BuilderStreamTab): boolean => {
    const builderViewToErrorPaths = getBuilderViewToErrorPaths(errorsRef.current);
    const viewFilteredViewToErrorPaths = limitToViews
      ? (Object.fromEntries(
          Object.entries(builderViewToErrorPaths).filter(([view]) => {
            const parsedView = !isNaN(Number(view)) ? Number(view) : view;
            return limitToViews.includes(parsedView as BuilderView);
          })
        ) as Record<BuilderView, string[]>)
      : builderViewToErrorPaths;

    if (limitToStreamTab) {
      return Object.values(viewFilteredViewToErrorPaths).some((errorPaths) =>
        errorPaths.some((errorPath) => getStreamTabFromErrorPath(errorPath) === limitToStreamTab)
      );
    }
    return Object.keys(viewFilteredViewToErrorPaths).length > 0;
  }, []);

  const getErrorPathAndView = useCallback(
    (limitToViews?: BuilderView[]): { view: BuilderView; errorPath: string } | undefined => {
      const builderViewToErrorPaths = getBuilderViewToErrorPaths(errorsRef.current);

      // if already on a view with an error, scroll to the first erroring field
      if ((!limitToViews || limitToViews.includes(view)) && builderViewToErrorPaths[view]) {
        return { view, errorPath: builderViewToErrorPaths[view][0] };
      }

      if (limitToViews) {
        const invalidViews = limitToViews.filter((view) => view in builderViewToErrorPaths);
        if (invalidViews.length === 0) {
          return undefined;
        }
        return { view: invalidViews[0], errorPath: builderViewToErrorPaths[invalidViews[0]][0] };
      }

      if (!limitToViews) {
        const viewToSelect = Object.keys(builderViewToErrorPaths)[0] as BuilderView;
        return {
          view: viewToSelect,
          errorPath: builderViewToErrorPaths[viewToSelect][0],
        };
      }

      return undefined;
    },
    [view]
  );

  const getOauthErrorPathAndView = useCallback(
    (limitToViews?: BuilderView[]): { view: BuilderView; errorPath: string } | undefined => {
      if (!limitToViews || limitToViews.includes("global")) {
        const authenticator = formValues?.global?.authenticator;
        if (!isString(authenticator) && authenticator?.type === DeclarativeOAuthAuthenticatorType) {
          const testingValues = getValues("testingValues") ?? {};

          const tokenType: "refresh" | "access" = !!authenticator.refresh_token_updater ? "refresh" : "access";
          const tokenConfigKey = extractInterpolatedConfigKey(
            tokenType === "refresh" ? authenticator.refresh_token : authenticator.access_token_value
          );

          if (!tokenConfigKey || !(tokenConfigKey in testingValues)) {
            return {
              view: "global",
              errorPath: "formValues.global.authenticator.declarative_oauth_flow",
            };
          }
        }
      }

      return undefined;
    },
    [formValues?.global?.authenticator, getValues]
  );

  const validateAndTouch = useCallback(
    (callback?: () => void, limitToViews?: BuilderView[]) => {
      trigger().then((isValid) => {
        if (isValid) {
          const oAuthErrorPathAndView = getOauthErrorPathAndView(limitToViews);
          if (oAuthErrorPathAndView) {
            setValue("view", oAuthErrorPathAndView.view);
            setScrollToField(oAuthErrorPathAndView.errorPath);
            return;
          }
          callback?.();
          return;
        }

        const errorPathAndView = getErrorPathAndView(limitToViews);
        if (errorPathAndView) {
          setValue("view", errorPathAndView.view);
          setScrollToField(errorPathAndView.errorPath);
          setValue("streamTab", getStreamTabFromErrorPath(errorPathAndView.errorPath));
          return;
        }

        const oAuthErrorPathAndView = getOauthErrorPathAndView(limitToViews);
        if (oAuthErrorPathAndView) {
          setValue("view", oAuthErrorPathAndView.view);
          setScrollToField(oAuthErrorPathAndView.errorPath);
          setValue("streamTab", getStreamTabFromErrorPath(oAuthErrorPathAndView.errorPath));
          return;
        }
        callback?.();
      });
    },
    [getErrorPathAndView, getOauthErrorPathAndView, setScrollToField, setValue, trigger]
  );

  return { hasErrors, validateAndTouch };
};

const getStreamTabFromErrorPath = (errorPath: string): BuilderStreamTab | undefined => {
  const pattern = /^formValues\.streams\.\d+\.([^.]+)/;
  const match = errorPath.match(pattern);
  const streamField = match ? match[1] : null;
  if (!streamField) {
    return undefined;
  }
  if (streamField === "schema") {
    return "schema";
  }
  if (streamField === "pollingRequester") {
    return "polling";
  }
  if (streamField === "downloadRequester") {
    return "download";
  }
  return "requester";
};

const getBuilderViewToErrorPaths = (errors: FieldErrors<BuilderState>) => {
  const result = {} as Record<BuilderView | "unknown", string[]>;

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
        // "global" or stream number if under formValues, or "inputs" if under testingValues
        const view: BuilderView | "unknown" =
          currentPath[0] === "formValues"
            ? currentPath[1] === "global"
              ? "global"
              : currentPath[1] === "streams"
              ? Number(currentPath[2])
              : "unknown"
            : currentPath[0] === "testingValues"
            ? "inputs"
            : "unknown";
        const fullPath = [...currentPath, key].join(".");
        if (!result[view]) {
          result[view] = [];
        }
        result[view].push(fullPath);
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
