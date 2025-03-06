import {
  EncryptionMapperConfiguration,
  FieldRenamingMapperConfiguration,
  HashingMapperConfiguration,
  MapperConfiguration,
  RowFilteringMapperConfiguration,
  RowFilteringOperation,
  RowFilteringOperationEqual,
  RowFilteringOperationEqualType,
  RowFilteringOperationNot,
  RowFilteringOperationNotType,
  StreamMapperType,
} from "core/api/types/AirbyteClient";

import { StreamMapperWithId } from "./types";

export const isEncryptionMapping = (
  mapping: StreamMapperWithId<MapperConfiguration>
): mapping is StreamMapperWithId<EncryptionMapperConfiguration> => {
  return mapping.type === StreamMapperType.encryption;
};

export const isHashingMapping = (
  mapping: StreamMapperWithId<MapperConfiguration>
): mapping is StreamMapperWithId<HashingMapperConfiguration> => {
  return mapping.type === StreamMapperType.hashing;
};

export const isFieldRenamingMapping = (
  mapping: StreamMapperWithId<MapperConfiguration>
): mapping is StreamMapperWithId<FieldRenamingMapperConfiguration> => {
  return mapping.type === StreamMapperType["field-renaming"];
};

export const isRowFilteringMapping = (
  mapping: StreamMapperWithId<MapperConfiguration>
): mapping is StreamMapperWithId<RowFilteringMapperConfiguration> => {
  return mapping.type === StreamMapperType["row-filtering"];
};
export function isRowFilteringOperationEqual(
  operation: RowFilteringOperation
): operation is RowFilteringOperationEqual {
  return operation.type === RowFilteringOperationEqualType.EQUAL;
}

export function isRowFilteringOperationNot(operation: RowFilteringOperation): operation is RowFilteringOperationNot {
  return operation.type === RowFilteringOperationNotType.NOT;
}
