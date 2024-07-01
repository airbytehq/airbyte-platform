import { OperationCreate, OperationRead, OperatorType } from "core/api/types/AirbyteClient";
export const isWebhookTransformation = (op: OperationCreate): op is OperationRead => {
  return op.operatorConfiguration.operatorType === OperatorType.webhook;
};
