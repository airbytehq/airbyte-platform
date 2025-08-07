import { useCallback } from "react";
import { useFormContext } from "react-hook-form";

import { DestinationOperation } from "core/api/types/AirbyteClient";

import { EMPTY_FIELD } from "./createFormDefaultValues";
import { getRequiredFields } from "./getRequiredFields";
import { DataActivationConnectionFormValues } from "../types";

export const useSetDefaultValuesForDestinationOperation = () => {
  const { setValue } = useFormContext<DataActivationConnectionFormValues>();

  return useCallback(
    (destinationOperation: DestinationOperation, streamIndex: number) => {
      const requiredDestinationFieldNames = getRequiredFields(destinationOperation);
      const requiredFieldMappings = requiredDestinationFieldNames.map((fieldName) => ({
        sourceFieldName: "",
        destinationFieldName: fieldName,
      }));
      const newFields = requiredFieldMappings.length === 0 ? [EMPTY_FIELD] : requiredFieldMappings;

      if (destinationOperation.matchingKeys?.length === 1) {
        // Auto-select the default matchingKeys value
        const autoSelectedMatchingKeys = destinationOperation.matchingKeys[0];
        const allRequiredFields = Array.from(new Set([...requiredDestinationFieldNames, ...autoSelectedMatchingKeys]));
        setValue(`streams.${streamIndex}.matchingKeys`, autoSelectedMatchingKeys);
        setValue(
          `streams.${streamIndex}.fields`,
          allRequiredFields.map((key) => ({
            sourceFieldName: "",
            destinationFieldName: key,
          }))
        );
      } else if (!destinationOperation.matchingKeys || destinationOperation.matchingKeys.length === 0) {
        // No matching keys are required
        setValue(`streams.${streamIndex}.matchingKeys`, null);
        setValue(`streams.${streamIndex}.fields`, newFields);
      } else {
        // Matching keys are required, but there are multiple options for the user to choose from
        setValue(`streams.${streamIndex}.matchingKeys`, []);
        setValue(`streams.${streamIndex}.fields`, newFields);
      }
    },
    [setValue]
  );
};
