import { DbtOperationRead } from "components/connection/TransformationForm";

import { OperationCreate, OperationRead, OperatorType } from "core/api/types/AirbyteClient";

export const isDbtTransformation = (op: OperationRead): op is DbtOperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.dbt;
};

export const isNormalizationTransformation = (op: OperationCreate): op is OperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.normalization;
};

export const isWebhookTransformation = (op: OperationCreate): op is OperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.webhook;
};
