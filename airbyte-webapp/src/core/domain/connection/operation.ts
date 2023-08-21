import { DbtOperationRead } from "components/connection/TransformationHookForm";

import { OperationCreate, OperationRead, OperatorType } from "core/request/AirbyteClient";

export enum NormalizationType {
  basic = "basic",
  raw = "raw",
}

export const isDbtTransformation = (op: OperationRead): op is DbtOperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.dbt;
};

export const isNormalizationTransformation = (op: OperationCreate): op is OperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.normalization;
};

export const isWebhookTransformation = (op: OperationCreate): op is OperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.webhook;
};
