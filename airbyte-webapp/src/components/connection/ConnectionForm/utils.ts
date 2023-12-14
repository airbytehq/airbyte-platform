import { NormalizationType } from "area/connection/types";
import { AirbyteStreamAndConfiguration, OperationCreate, OperatorType } from "core/api/types/AirbyteClient";

import { FormConnectionFormValues } from "./formConfig";

/**
 * since AirbyteStreamAndConfiguration don't have a unique identifier
 * we need to compare the name and namespace to determine if they are the same stream
 * @param streamNode
 * @param streamName
 * @param streamNamespace
 */
export const isSameSyncStream = (
  streamNode: AirbyteStreamAndConfiguration,
  streamName: string | undefined,
  streamNamespace: string | undefined
) => streamNode.stream?.name === streamName && streamNode.stream?.namespace === streamNamespace;
/**
 * map the normalization option to the operation
 */
const mapNormalizationOptionToOperation = (
  workspaceId: string,
  normalization?: NormalizationType
): OperationCreate[] => {
  // if normalization is not supported OR normalization is supported but the default value is selected need to return empty array
  if (!normalization || normalization === NormalizationType.raw) {
    return [];
  }

  // otherwise return the normalization operation selected value - "basic"
  return [
    {
      name: "Normalization",
      workspaceId,
      operatorConfiguration: {
        operatorType: OperatorType.normalization,
        normalization: {
          option: normalization,
        },
      },
    },
  ];
};

/**
 * we need to combine the normalizations, custom transformations and dbt transformations in one operations array
 * this function will take the form values return the operations array
 * used in create connection case
 */
export const mapFormValuesToOperations = (
  workspaceId: string,
  normalization: FormConnectionFormValues["normalization"],
  transformations: FormConnectionFormValues["transformations"]
): OperationCreate[] => {
  const normalizationOperation = mapNormalizationOptionToOperation(workspaceId, normalization);

  return [...normalizationOperation, ...(transformations?.length ? transformations : [])];
};
