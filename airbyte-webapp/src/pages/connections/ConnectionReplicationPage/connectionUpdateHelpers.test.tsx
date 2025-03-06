import {
  DestinationSyncMode,
  SyncMode,
  StreamMapperType,
  HashingMapperConfigurationMethod,
  RowFilteringOperationType,
  EncryptionMapperAlgorithm,
  MapperConfiguration,
} from "core/api/types/AirbyteClient";

import { determineRecommendRefresh } from "./connectionUpdateHelpers";

const createStream = (overrides = {}) => ({
  stream: { name: "exampleStream", namespace: "exampleNamespace" },
  config: {
    aliasName: "exampleStream",
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.overwrite,
    selectedFields: [{ fieldPath: ["field1"] }],
    primaryKey: [["key1"]],
    cursorField: ["field1"],
    selected: true,
    mappers: [],
    ...overrides,
  },
});

describe("#determineRecommendRefresh", () => {
  describe("sync mode changes", () => {
    it.each`
      description                                                                        | storedSyncMode           | storedDestSyncMode               | formSyncMode             | formDestSyncMode                    | expected
      ${"recommend if full refresh | overwrite changing to incremental | append"}        | ${SyncMode.full_refresh} | ${DestinationSyncMode.overwrite} | ${SyncMode.incremental}  | ${DestinationSyncMode.append}       | ${true}
      ${"recommend if full refresh | overwrite changing to incremental | append_dedupe"} | ${SyncMode.full_refresh} | ${DestinationSyncMode.overwrite} | ${SyncMode.incremental}  | ${DestinationSyncMode.append_dedup} | ${true}
      ${"does not recommend if incremental to full_refresh"}                             | ${SyncMode.incremental}  | ${DestinationSyncMode.append}    | ${SyncMode.full_refresh} | ${DestinationSyncMode.append}       | ${false}
      ${"does not recommend if changing between incremental types"}                      | ${SyncMode.incremental}  | ${DestinationSyncMode.append}    | ${SyncMode.incremental}  | ${DestinationSyncMode.append_dedup} | ${false}
    `("$description", ({ storedSyncMode, storedDestSyncMode, formSyncMode, formDestSyncMode, expected }) => {
      const storedSyncCatalog = {
        streams: [createStream({ syncMode: storedSyncMode, destinationSyncMode: storedDestSyncMode })],
      };
      const formSyncCatalog = {
        streams: [createStream({ syncMode: formSyncMode, destinationSyncMode: formDestSyncMode })],
      };
      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(expected);
    });
  });

  describe("field changes", () => {
    it.each`
      description                                                      | storedOverrides                                                                              | formOverrides                                                                                                                                                   | expected
      ${"change in primary key suggests refresh if incremental dedup"} | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append_dedup }} | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append_dedup, primaryKey: [["key2"]] }}                                            | ${true}
      ${"ignores pk changes if not incremental"}                       | ${{}}                                                                                        | ${{ primaryKey: [["key2"]] }}                                                                                                                                   | ${false}
      ${"change in cursor field suggests refresh if incremental"}      | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append }}       | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append, cursorField: ["cursor2"] }}                                                | ${true}
      ${"ignores cursor change if not incremental"}                    | ${{ syncMode: SyncMode.full_refresh, destinationSyncMode: DestinationSyncMode.append }}      | ${{ syncMode: SyncMode.full_refresh, destinationSyncMode: DestinationSyncMode.append, cursorField: ["cursor2"] }}                                               | ${false}
      ${"changes in selected fields suggest refresh if incremental"}   | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append }}       | ${{ syncMode: SyncMode.incremental, destinationSyncMode: DestinationSyncMode.append, selectedFields: [{ fieldPath: ["field1"] }, { fieldPath: ["field2"] }] }}  | ${true}
      ${"ignores changes in selected fields if not incremental"}       | ${{ syncMode: SyncMode.full_refresh, destinationSyncMode: DestinationSyncMode.append }}      | ${{ syncMode: SyncMode.full_refresh, destinationSyncMode: DestinationSyncMode.append, selectedFields: [{ fieldPath: ["field1"] }, { fieldPath: ["field2"] }] }} | ${false}
    `("$description", ({ storedOverrides, formOverrides, expected }) => {
      const storedSyncCatalog = { streams: [createStream(storedOverrides)] };
      const formSyncCatalog = { streams: [createStream(formOverrides)] };
      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(expected);
    });
  });

  describe("mappers changes", () => {
    const createMapper = (id: string, type: StreamMapperType, config: MapperConfiguration) => ({
      id,
      type,
      mapperConfiguration: config,
    });

    it.each`
      description                                                                                         | storedMappers                                                                                                                                                                        | formMappers                                                                                                                                                                                                                                                                                                                         | expected
      ${"should recommend refresh if a new hashing mapper is added"}                                      | ${[]}                                                                                                                                                                                | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                                                                                                                                                                          | ${true}
      ${"should recommend refresh if a hashing mapper's target field is changed"}                         | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                           | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "newField", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                                                                                                                                                                       | ${true}
      ${"should not recommend refresh if a hashing mapper's other property is changed"}                   | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                           | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field", method: HashingMapperConfigurationMethod["SHA-256"], fieldNameSuffix: "_hashed" })]}                                                                                                                                                                   | ${false}
      ${"should recommend refresh if a hashing mapper is removed"}                                        | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                           | ${[]}                                                                                                                                                                                                                                                                                                                               | ${true}
      ${"should recommend refresh if a new encryption mapper is added"}                                   | ${[]}                                                                                                                                                                                | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "myCoolKey" })]}                                                                                                                                                | ${true}
      ${"should recommend refresh if an encryption mapper's target field is changed"}                     | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "myCoolKey" })]} | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "different_field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "myCoolKey" })]}                                                                                                                                      | ${true}
      ${"should not recommend refresh if an encryption mapper's other property is changed"}               | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "myCoolKey" })]} | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "anotherCoolKey" })]}                                                                                                                                           | ${false}
      ${"should recommend refresh if an encryption mapper is removed"}                                    | ${[createMapper("mapper1", StreamMapperType.encryption, { targetField: "field", algorithm: EncryptionMapperAlgorithm.RSA, fieldNameSuffix: "_encrypted", publicKey: "myCoolKey" })]} | ${[]}                                                                                                                                                                                                                                                                                                                               | ${true}
      ${"should recommend refresh if a field renaming mapper is added"}                                   | ${[]}                                                                                                                                                                                | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "field", newFieldName: "new_field" })]}                                                                                                                                                                                                         | ${true}
      ${"should recommend refresh if a field renaming mapper originalField is changed"}                   | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "field", newFieldName: "new_field" })]}                                                          | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "other-field", newFieldName: "new_field" })]}                                                                                                                                                                                                   | ${true}
      ${"should recommend refresh if a field renaming mapper newFieldName is changed"}                    | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "field", newFieldName: "new_field" })]}                                                          | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "field", newFieldName: "new_field_also" })]}                                                                                                                                                                                                    | ${true}
      ${"should recommend refresh if a field renaming mapper is removed"}                                 | ${[createMapper("mapper1", StreamMapperType["field-renaming"], { originalFieldName: "field", newFieldName: "new_field" })]}                                                          | ${[]}                                                                                                                                                                                                                                                                                                                               | ${true}
      ${"should not recommend refresh if a new row filtering mapper is added"}                            | ${[]}                                                                                                                                                                                | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } })]}                                                                                                                                                          | ${false}
      ${"should not recommend refresh if a row filtering mapper is changed"}                              | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } })]}           | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field3" } })]}                                                                                                                                                          | ${false}
      ${"should not recommend refresh if a new row filtering mapper is removed"}                          | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } })]}           | ${[]}                                                                                                                                                                                                                                                                                                                               | ${false}
      ${"should recommend refresh if a new mapper is added and a row filter one exists"}                  | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } })]}           | ${[createMapper("mapper1", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } }), createMapper("mapper2", StreamMapperType.hashing, { targetField: "field_2", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]} | ${true}
      ${"should not recommend refresh if a new row filter mapper is added even if another mapper exists"} | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field_2", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" })]}                         | ${[createMapper("mapper1", StreamMapperType.hashing, { targetField: "field_2", method: HashingMapperConfigurationMethod.MD5, fieldNameSuffix: "_hashed" }), createMapper("mapper2", StreamMapperType["row-filtering"], { conditions: { type: RowFilteringOperationType.EQUAL, comparisonValue: "banana", fieldName: "field1" } })]} | ${false}
    `("$description", ({ storedMappers, formMappers, expected }) => {
      const storedSyncCatalog = { streams: [createStream({ mappers: storedMappers })] };
      const formSyncCatalog = { streams: [createStream({ mappers: formMappers })] };
      const result = determineRecommendRefresh(formSyncCatalog, storedSyncCatalog);
      expect(result).toBe(expected);
    });
  });
});
