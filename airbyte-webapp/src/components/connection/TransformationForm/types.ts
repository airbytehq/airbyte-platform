import { OperationId, OperatorDbt } from "core/request/AirbyteClient";

export interface DbtOperationRead {
  name: string;
  workspaceId: string;
  operationId: OperationId;
  operatorConfiguration: {
    operatorType: "dbt";
    dbt: OperatorDbt;
  };
}

export interface DbtOperationReadOrCreate extends Omit<DbtOperationRead, "operationId"> {
  operationId?: OperationId;
}
