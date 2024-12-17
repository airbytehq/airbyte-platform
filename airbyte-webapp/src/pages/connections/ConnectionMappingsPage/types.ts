import {
  EncryptionMapperAlgorithm,
  EncryptionMapperAESConfiguration,
  EncryptionMapperRSAConfiguration,
  MapperConfiguration,
  ConfiguredStreamMapper,
  RowFilteringOperationEqual,
  RowFilteringOperationNot,
} from "core/api/types/AirbyteClient";

export type EncryptionMapperConfiguration<T extends EncryptionMapperAlgorithm = EncryptionMapperAlgorithm> =
  T extends "RSA"
    ? EncryptionMapperRSAConfiguration
    : T extends "AES"
    ? EncryptionMapperAESConfiguration
    : EncryptionMapperRSAConfiguration | EncryptionMapperAESConfiguration;

export type RowFilteringMapperConfiguration<T extends "OUT" | "IN" = "OUT" | "IN"> = T extends "OUT"
  ? {
      conditions: RowFilteringOperationNot;
    }
  : T extends "IN"
  ? {
      conditions: RowFilteringOperationEqual;
    }
  : { conditions: RowFilteringOperationNot | RowFilteringOperationEqual };

export interface StreamMapperWithId<
  T extends MapperConfiguration = MapperConfiguration,
  U extends EncryptionMapperAlgorithm = EncryptionMapperAlgorithm, // Default `U` to the union of all algorithms
  V extends "OUT" | "IN" = "OUT" | "IN", // Default V to a union of all possible filter types
> extends ConfiguredStreamMapper {
  id: string;
  validationCallback: () => Promise<boolean>;
  mapperConfiguration: T extends EncryptionMapperConfiguration<U>
    ? EncryptionMapperConfiguration<U>
    : T extends RowFilteringMapperConfiguration<V>
    ? RowFilteringMapperConfiguration<V>
    : T;
}
