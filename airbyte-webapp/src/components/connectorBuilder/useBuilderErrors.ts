import intersection from "lodash/intersection";
import { useCallback } from "react";
import { FieldErrors, set, useFormContext, useFormState } from "react-hook-form";

import {
  BuilderView,
  useConnectorBuilderFormManagementState,
  useConnectorBuilderFormState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderFormValues, builderFormValidationSchema } from "./types";

export const useBuilderErrors = () => {
  const { trigger, getValues } = useFormContext();
  const { errors } = useFormState<BuilderFormValues>();
  const { setSelectedView } = useConnectorBuilderFormState();
  const { setScrollToField } = useConnectorBuilderFormManagementState();

  const invalidViews = useCallback(
    (limitToViews?: BuilderView[], inputErrors?: FieldErrors<BuilderFormValues>) => {
      const errorsToCheck = inputErrors !== undefined ? inputErrors : errors;
      const errorKeys = Object.keys(errorsToCheck).filter((errorKey) =>
        Boolean(errorsToCheck[errorKey as keyof BuilderFormValues])
      );

      const invalidViews: BuilderView[] = [];

      if (errorKeys.includes("global")) {
        invalidViews.push("global");
      }

      if (errorKeys.includes("streams") && typeof errorsToCheck.streams === "object") {
        const errorStreamNums = Object.keys(errorsToCheck.streams ?? {}).filter((errorKey) =>
          Boolean(errorsToCheck.streams?.[Number(errorKey)])
        );

        invalidViews.push(...errorStreamNums.map((numString) => Number(numString)));
      }

      return limitToViews === undefined ? invalidViews : intersection(invalidViews, limitToViews);
    },
    [errors]
  );

  // Returns true if the global config fields or any stream config fields have errors in the provided rhf errors, and false otherwise.
  // If limitToViews is provided, the error check is limited to only those views.
  const hasErrors = useCallback(
    (limitToViews?: BuilderView[]) => {
      return invalidViews(limitToViews).length > 0;
    },
    [invalidViews]
  );

  const validateAndTouch = useCallback(
    (callback?: () => void, limitToViews?: BuilderView[]) => {
      trigger().then((isValid) => {
        if (isValid) {
          callback?.();
          return;
        }
        const currentValues = getValues();
        let firstErrorPath: string | undefined = undefined;
        try {
          builderFormValidationSchema.validateSync(currentValues);
        } catch (e) {
          firstErrorPath = e.path;
        }
        if (!firstErrorPath) {
          return;
        }

        const errorObject = {};
        set(errorObject, firstErrorPath, "error");

        const invalidBuilderViews = invalidViews(limitToViews, errorObject);

        if (invalidBuilderViews.length > 0) {
          setScrollToField(firstErrorPath);
          if (invalidBuilderViews.includes("global")) {
            setSelectedView("global");
          } else {
            setSelectedView(invalidBuilderViews[0]);
          }
        }
      });
    },
    [getValues, invalidViews, setScrollToField, setSelectedView, trigger]
  );

  return { hasErrors, validateAndTouch };
};
