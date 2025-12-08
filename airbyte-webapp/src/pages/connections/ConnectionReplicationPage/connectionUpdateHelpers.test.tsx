import { ConnectionValues } from "core/api";
import {
  DestinationSyncMode,
  SyncMode,
  StreamMapperType,
  HashingMapperConfigurationMethod,
  RowFilteringOperationType,
  EncryptionMapperAlgorithm,
  MapperConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
} from "core/api/types/AirbyteClient";

import {
  determineConnectionUpdateActions,
  determineRecommendRefresh,
  discardFullRefreshChanges,
  getFullRefreshHighFrequencyWarningInfo,
  getRecommendRefreshStreams,
  UserDecisions,
} from "./connectionUpdateHelpers";

const createStream = (configOverrides = {}, streamOverrides = {}) => ({
  stream: { name: "exampleStream", namespace: "exampleNamespace", ...streamOverrides },
  config: {
    aliasName: "exampleStream",
    syncMode: SyncMode.full_refresh,
    destinationSyncMode: DestinationSyncMode.overwrite,
    selectedFields: [{ fieldPath: ["field1"] }],
    primaryKey: [["key1"]],
    cursorField: ["field1"],
    selected: true,
    mappers: [],
    ...configOverrides,
  },
});

const createFullRefreshStream = (
  configOverrides = {},
  streamOverrides = { name: "exampleStream", namespace: "exampleNamespace" }
) =>
  createStream(
    {
      syncMode: SyncMode.full_refresh,
      ...configOverrides,
    },
    streamOverrides
  );

const createIncrementalStream = (
  configOverrides = {},
  streamOverrides = { name: "exampleStream", namespace: "exampleNamespace" }
) =>
  createStream(
    {
      syncMode: SyncMode.incremental,
      ...configOverrides,
    },
    streamOverrides
  );

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

describe("#getRecommendRefreshStreams", () => {
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
      const { shouldRefreshWarning } = getRecommendRefreshStreams(formSyncCatalog, storedSyncCatalog);
      expect(shouldRefreshWarning).toBe(expected);
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
      const { shouldRefreshWarning } = getRecommendRefreshStreams(formSyncCatalog, storedSyncCatalog);
      expect(shouldRefreshWarning).toBe(expected);
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
      const { shouldRefreshWarning } = getRecommendRefreshStreams(formSyncCatalog, storedSyncCatalog);
      expect(shouldRefreshWarning).toBe(expected);
    });
  });
});

describe("#getFullRefreshHighFrequencyWarningInfo", () => {
  describe("high-frequency schedule detection", () => {
    it("should warn for 1-hour basic schedule with 2+ streams", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(2);
    });

    it("should warn for 12-hour basic schedule with 2+ streams", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 12, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(2);
    });

    it("should NOT warn for 24-hour basic schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 24, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should NOT warn for daily basic schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "days" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should NOT warn for weekly basic schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream()],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream()],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "weeks" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should NOT warn for monthly basic schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream()],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream()],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "months" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should NOT warn for manual schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData: undefined,
        scheduleType: ConnectionScheduleType.manual,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should NOT warn when scheduleType is undefined", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream()],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream()],
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData: undefined,
        scheduleType: undefined,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });
  });

  describe("stream change detection", () => {
    it("should count 2 streams changed to full refresh", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" } })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(affectedStreams.length).toBe(2);
    });

    it("should warn for 1 stream changed to full refresh", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream()],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream()],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(1);
    });

    it("should skip streams that are not selected", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream(), createIncrementalStream({ stream: { name: "stream2" }, selected: false })],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream(), createFullRefreshStream({ stream: { name: "stream2" }, selected: false })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(1);
    });

    it("should count only streams actually changing from incremental to full refresh", () => {
      const itemsStream = createFullRefreshStream({}, { name: "items", namespace: "public" });
      const storedSyncCatalog = {
        streams: [itemsStream, createIncrementalStream({ stream: { name: "stream2" } })],
      };

      const formSyncCatalog = {
        streams: [itemsStream, createFullRefreshStream({ stream: { name: "stream2" } })],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 4, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(1);
    });

    it("should count only streams changing from incremental to full refresh", () => {
      const storedSyncCatalog = {
        streams: [
          createIncrementalStream({ stream: { name: "stream1" } }, { name: "items", namespace: "public" }),
          createIncrementalStream({ stream: { name: "stream2" } }, { name: "stream2", namespace: "public" }),
          createFullRefreshStream({ stream: { name: "stream3" } }),
        ],
      };
      const formSyncCatalog = {
        streams: [
          createFullRefreshStream({ stream: { name: "stream1" } }, { name: "items", namespace: "public" }),
          createFullRefreshStream({ stream: { name: "stream2" } }, { name: "stream2", namespace: "public" }),
          createFullRefreshStream({ stream: { name: "stream3" } }),
        ],
      };

      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 4, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(2);
    });

    it("should handle streams with namespace in stream ID", () => {
      const storedSyncCatalog = {
        streams: [
          createIncrementalStream(),
          createIncrementalStream({ stream: { name: "stream2", namespace: "custom_namespace" } }),
        ],
      };
      const formSyncCatalog = {
        streams: [
          createFullRefreshStream(),
          createFullRefreshStream({ stream: { name: "stream2", namespace: "custom_namespace" } }),
        ],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(2);
    });
  });

  describe("combined scenarios", () => {
    it("should warn: 3 streams changed to full refresh + hourly schedule", () => {
      const storedSyncCatalog = {
        streams: [
          createIncrementalStream(),
          createIncrementalStream({ stream: { name: "stream2" } }),
          createIncrementalStream({ stream: { name: "stream3" } }),
        ],
      };
      const formSyncCatalog = {
        streams: [
          createFullRefreshStream(),
          createFullRefreshStream({ stream: { name: "stream2" } }),
          createFullRefreshStream({ stream: { name: "stream3" } }),
        ],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(3);
    });

    it("should NOT warn: 3 streams changed to full refresh + daily schedule", () => {
      const storedSyncCatalog = {
        streams: [
          createIncrementalStream(),
          createIncrementalStream({ stream: { name: "stream2" } }),
          createIncrementalStream({ stream: { name: "stream3" } }),
        ],
      };
      const formSyncCatalog = {
        streams: [
          createFullRefreshStream(),
          createFullRefreshStream({ stream: { name: "stream2" } }),
          createFullRefreshStream({ stream: { name: "stream3" } }),
        ],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "days" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(false);
      expect(affectedStreams.length).toBe(0);
    });

    it("should warn: 1 stream changed to full refresh + hourly schedule", () => {
      const storedSyncCatalog = {
        streams: [createIncrementalStream()],
      };
      const formSyncCatalog = {
        streams: [createFullRefreshStream()],
      };
      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 1, timeUnit: "hours" },
      };

      const { highFrequencyWarning, affectedStreams } = getFullRefreshHighFrequencyWarningInfo({
        formSyncCatalog,
        storedSyncCatalog,
        scheduleData,
        scheduleType: ConnectionScheduleType.basic,
      });

      expect(highFrequencyWarning).toBe(true);
      expect(affectedStreams.length).toBe(1);
    });
  });
});

describe("#determineConnectionUpdateActions", () => {
  describe("fullRefreshHighFrequency decisions", () => {
    it("should set skipReset=true when fullRefreshHighFrequency is accepted", () => {
      const decisions: UserDecisions = { fullRefreshHighFrequency: "accept" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(true);
      expect(actions.fullRefreshStreamsToRevert).toBeUndefined();
    });

    it("should revert streams when fullRefreshHighFrequency is rejected", () => {
      const stream1 = createStream(
        { stream: { name: "stream1", namespace: "ns" } },
        { name: "stream1", namespace: "public" }
      );
      const stream2 = createStream(
        { stream: { name: "stream2", namespace: "ns" } },
        { name: "stream2", namespace: "public" }
      );

      const changes = {
        fullRefreshHighFrequency: [
          {
            id: "high-freq-stream1",
            streamName: "stream1",
            storedConfig: stream1,
          },
          {
            id: "high-freq-stream2",
            streamName: "stream2",
            storedConfig: stream2,
          },
        ],
      };

      const decisions: UserDecisions = { fullRefreshHighFrequency: "reject" };
      const actions = determineConnectionUpdateActions(decisions, changes);

      expect(actions.skipReset).toBe(true);
      expect(actions.fullRefreshStreamsToRevert).toHaveLength(2);
      expect(actions.fullRefreshStreamsToRevert?.[0].stream?.name).toBe("stream1");
      expect(actions.fullRefreshStreamsToRevert?.[1].stream?.name).toBe("stream2");
    });
  });

  describe("refresh/clear decisions", () => {
    it("should set skipReset=false when refresh is accepted", () => {
      const decisions: UserDecisions = { refresh: "accept" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(false);
    });

    it("should set skipReset=false when clear is accepted", () => {
      const decisions: UserDecisions = { clear: "accept" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(false);
    });

    it("should set skipReset=true when refresh is rejected", () => {
      const decisions: UserDecisions = { refresh: "reject" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(true);
    });

    it("should set skipReset=true when clear is rejected", () => {
      const decisions: UserDecisions = { clear: "reject" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(true);
    });
  });

  describe("combined decisions", () => {
    it("should handle multiple decisions - fullRefreshHighFrequency accept + clear reject", () => {
      const decisions: UserDecisions = { fullRefreshHighFrequency: "accept", clear: "reject" };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(true);
      expect(actions.fullRefreshStreamsToRevert).toBeUndefined();
    });

    it("should handle multiple decisions - fullRefreshHighFrequency reject + refresh accept", () => {
      const stream1 = createStream();
      const changes = {
        fullRefreshHighFrequency: [
          {
            id: "high-freq-stream1",
            streamName: "exampleStream",
            storedConfig: stream1,
          },
        ],
      };

      const decisions: UserDecisions = { fullRefreshHighFrequency: "reject", refresh: "accept" };
      const actions = determineConnectionUpdateActions(decisions, changes);

      expect(actions.skipReset).toBe(false);
      expect(actions.fullRefreshStreamsToRevert).toHaveLength(1);
    });

    it("should prioritize refresh/clear accept over fullRefreshHighFrequency for skipReset", () => {
      const decisions: UserDecisions = {
        fullRefreshHighFrequency: "reject",
        clear: "accept",
      };
      const actions = determineConnectionUpdateActions(decisions);

      expect(actions.skipReset).toBe(false);
    });
  });
});

describe("#discardFullRefreshChanges", () => {
  describe("full reversion", () => {
    it("should revert all stream configuration to stored state", () => {
      const formStream = createStream({
        syncMode: SyncMode.full_refresh,
        destinationSyncMode: DestinationSyncMode.overwrite,
        selectedFields: [{ fieldPath: ["field1"] }, { fieldPath: ["field2"] }],
        cursorField: ["newCursor"],
      });

      const storedStream = createStream({
        syncMode: SyncMode.incremental,
        destinationSyncMode: DestinationSyncMode.append,
        selectedFields: [{ fieldPath: ["field1"] }],
        cursorField: ["field1"],
      });

      const values = {
        syncCatalog: { streams: [formStream] },
        scheduleType: ConnectionScheduleType.basic,
        namespaceDefinition: "source",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [storedStream]);

      const resultStream = result.syncCatalog.streams[0];
      expect(resultStream.config?.syncMode).toBe(SyncMode.incremental);
      expect(resultStream.config?.destinationSyncMode).toBe(DestinationSyncMode.append);
      expect(resultStream.config?.selectedFields).toEqual([{ fieldPath: ["field1"] }]);
      expect(resultStream.config?.cursorField).toEqual(["field1"]);
    });

    it("should revert all changes including primary key and hashed fields", () => {
      const formStream = createStream({
        syncMode: SyncMode.full_refresh,
        destinationSyncMode: DestinationSyncMode.overwrite,
        primaryKey: [["newKey"]],
        hashedFields: [{ fieldPath: ["sensitive"] }],
      });

      const storedStream = createStream({
        syncMode: SyncMode.incremental,
        destinationSyncMode: DestinationSyncMode.append,
        primaryKey: [["id"]],
        hashedFields: [],
      });

      const values = {
        syncCatalog: { streams: [formStream] },
        scheduleType: ConnectionScheduleType.basic,
        namespaceDefinition: "source",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [storedStream]);

      const resultStream = result.syncCatalog.streams[0];
      expect(resultStream.config?.syncMode).toBe(SyncMode.incremental);
      expect(resultStream.config?.destinationSyncMode).toBe(DestinationSyncMode.append);
      expect(resultStream.config?.primaryKey).toEqual([["id"]]);
      expect(resultStream.config?.hashedFields).toEqual([]);
    });

    it("should handle multiple streams and revert only matching ones", () => {
      const stream1Form = createFullRefreshStream(
        { stream: { name: "stream1" } },
        { name: "stream1", namespace: "public" }
      );
      const stream2Form = createFullRefreshStream(
        { stream: { name: "stream2" } },
        { name: "stream2", namespace: "public" }
      );
      const stream1Stored = createIncrementalStream(
        { stream: { name: "stream1" } },
        { name: "stream1", namespace: "public" }
      );

      const values = {
        syncCatalog: { streams: [stream1Form, stream2Form] },
        scheduleType: ConnectionScheduleType.basic,
        namespaceDefinition: "source",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [stream1Stored]);

      expect(result.syncCatalog.streams[0].config?.syncMode).toBe(SyncMode.incremental);
      expect(result.syncCatalog.streams[1].config?.syncMode).toBe(SyncMode.full_refresh); // Unchanged
    });
  });

  describe("form value preservation", () => {
    it("should preserve prefix when reverting streams", () => {
      const formStream = createFullRefreshStream();
      const storedStream = createIncrementalStream();

      const values = {
        syncCatalog: { streams: [formStream] },
        scheduleType: ConnectionScheduleType.basic,
        namespaceDefinition: "source",
        prefix: "my_prefix",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [storedStream]);

      expect(result.prefix).toBe("my_prefix");
    });

    it("should preserve namespaceDefinition when reverting streams", () => {
      const formStream = createFullRefreshStream();
      const storedStream = createIncrementalStream();

      const values = {
        syncCatalog: { streams: [formStream] },
        scheduleType: ConnectionScheduleType.basic,
        namespaceDefinition: "customformat",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [storedStream]);

      expect(result.namespaceDefinition).toBe("customformat");
    });

    it("should preserve all form fields including schedule when reverting streams", () => {
      const formStream = createFullRefreshStream();
      const storedStream = createIncrementalStream();

      const scheduleData: ConnectionScheduleData = {
        basicSchedule: { units: 24, timeUnit: "hours" },
      };

      const values = {
        syncCatalog: { streams: [formStream] },
        name: "my-connection",
        scheduleType: ConnectionScheduleType.basic,
        scheduleData,
        namespaceDefinition: "customformat",

        prefix: "test_prefix",
      } as ConnectionValues;
      const result = discardFullRefreshChanges(values, [storedStream]);

      expect(result.name).toBe("my-connection");
      expect(result.scheduleType).toBe(ConnectionScheduleType.basic);
      expect(result.scheduleData).toEqual(scheduleData);
      expect(result.namespaceDefinition).toBe("customformat");
      expect(result.prefix).toBe("test_prefix");
    });
  });
});
