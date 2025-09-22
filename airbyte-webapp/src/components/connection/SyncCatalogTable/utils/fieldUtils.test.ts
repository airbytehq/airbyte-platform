import { SyncStreamFieldWithId } from "components/connection/ConnectionForm/formConfig";

import {
  AirbyteStreamAndConfiguration,
  DestinationSyncMode,
  HashingMapperConfigurationMethod,
  StreamMapperType,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import { getFieldChangeStatus, checkIsFieldHashed, checkIsFieldSelected, flattenSyncSchemaFields } from "./fieldUtils";

const FIELD_ONE: SyncSchemaField = {
  path: ["field_one"],
  cleanedName: "field_one",
  key: "field_one",
  type: "string",
};
const FIELD_TWO: SyncSchemaField = {
  path: ["field_two"],
  cleanedName: "field_two",
  key: "field_two",
  type: "string",
};

const NESTED_FIELD = {
  path: ["field_three", "nestedField"],
  cleanedName: "nestedField",
  key: "nestedField",
  type: "string",
};

const FIELD_THREE: SyncSchemaField = {
  cleanedName: "field_three",
  type: "todo",
  key: "field_three",
  path: ["field_three"],
  fields: [NESTED_FIELD],
};

const mockedInitialStream: AirbyteStreamAndConfiguration = {
  stream: { name: "stream1", namespace: "namespace1" },
  config: { selected: true, destinationSyncMode: DestinationSyncMode.append_dedup, syncMode: SyncMode.incremental },
};

const mockedStreamNode: SyncStreamFieldWithId = {
  id: "1",
  ...mockedInitialStream,
};

/**
 * flattenSyncSchemaFields function tests
 */
describe(`${flattenSyncSchemaFields.name}`, () => {
  it("flattens a single level array of fields", () => {
    const fields = [FIELD_ONE, FIELD_TWO];
    const result = flattenSyncSchemaFields(fields);
    expect(result).toEqual(fields);
  });

  it("flattens a nested array of fields", () => {
    const fields = [FIELD_THREE];
    const result = flattenSyncSchemaFields(fields);
    expect(result).toEqual([FIELD_THREE, NESTED_FIELD]);
  });

  it("returns an empty array when given an empty array", () => {
    const fields: SyncSchemaField[] = [];
    const result = flattenSyncSchemaFields(fields);
    expect(result).toEqual([]);
  });

  it("flattens a complex nested array of fields", () => {
    const nestedField = {
      path: ["field_four", "nestedField"],
      cleanedName: "nestedField",
      key: "nestedField",
      type: "string",
    };
    const fieldFour: SyncSchemaField = {
      cleanedName: "field_four",
      type: "todo",
      key: "field_four",
      path: ["field_four"],
      fields: [nestedField],
    };
    const fields = [FIELD_ONE, fieldFour];
    const result = flattenSyncSchemaFields(fields);
    expect(result).toEqual([FIELD_ONE, fieldFour, nestedField]);
  });
});

/**
 * checkIsFieldSelected function tests
 */
describe(`${checkIsFieldSelected.name}`, () => {
  it("returns false when stream is not selected", () => {
    const config = { ...mockedStreamNode.config!, selected: false };
    const result = checkIsFieldSelected(FIELD_ONE, config);
    expect(result).toBe(false);
  });

  it("returns true when field selection is disabled", () => {
    const config = { ...mockedStreamNode.config!, fieldSelectionEnabled: false };
    const result = checkIsFieldSelected(FIELD_ONE, config);
    expect(result).toBe(true);
  });

  it("returns true when the field is in the selected fields", () => {
    const config = {
      ...mockedStreamNode.config!,
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: FIELD_ONE.path }],
    };
    const result = checkIsFieldSelected(FIELD_ONE, config);
    expect(result).toBe(true);
  });

  it("returns false when the field is not in the selected fields", () => {
    const config = {
      ...mockedStreamNode.config!,
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: FIELD_TWO.path }],
    };
    const result = checkIsFieldSelected(FIELD_ONE, config);
    expect(result).toBe(false);
  });
});

/**
 * getFieldChangeStatus function tests
 */
describe(`${getFieldChangeStatus.name}`, () => {
  it("returns 'disabled' when stream is not selected", () => {
    const result = getFieldChangeStatus(
      mockedInitialStream,
      {
        ...mockedStreamNode,
        config: {
          ...mockedStreamNode.config!,
          selected: false,
          fieldSelectionEnabled: true,
          selectedFields: [{ fieldPath: FIELD_ONE.path }],
        },
      },
      FIELD_ONE
    );
    expect(result).toBe("disabled");
  });

  it("returns 'unchanged' for nested fields", () => {
    const result = getFieldChangeStatus(mockedInitialStream, mockedStreamNode, NESTED_FIELD);
    expect(result).toBe("unchanged");
  });

  it("returns 'added' when field is selected but was not initially selected", () => {
    const initialStream: AirbyteStreamAndConfiguration = {
      ...mockedInitialStream,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }],
      },
    };

    const streamNode = {
      ...mockedStreamNode,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }], // field_two is added
      },
    };

    const resultFieldOne = getFieldChangeStatus(initialStream, streamNode, FIELD_ONE);
    const resultFieldTwo = getFieldChangeStatus(initialStream, streamNode, FIELD_TWO);

    expect(resultFieldOne).toBe("unchanged");
    expect(resultFieldTwo).toBe("added");
  });

  // corner case - all fields are selected
  it("returns 'added' when last field is selected but was not initially selected", () => {
    const initialStream: AirbyteStreamAndConfiguration = {
      ...mockedInitialStream,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      },
    };

    const streamNode = {
      ...mockedStreamNode,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: false, // all fields are selected
      },
    };

    const resultFieldOne = getFieldChangeStatus(initialStream, streamNode, FIELD_ONE);
    const resultFieldTwo = getFieldChangeStatus(initialStream, streamNode, FIELD_TWO);
    const resultFieldThree = getFieldChangeStatus(initialStream, streamNode, FIELD_THREE);

    expect(resultFieldOne).toBe("unchanged");
    expect(resultFieldTwo).toBe("unchanged");
    expect(resultFieldThree).toBe("added");
  });

  it("returns 'removed' when field is not selected but was initially selected", () => {
    const initialStream: AirbyteStreamAndConfiguration = {
      ...mockedInitialStream,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      },
    };

    const streamNode = {
      ...mockedStreamNode,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_TWO.path }], // field_one is removed
      },
    };

    const resultFieldOne = getFieldChangeStatus(initialStream, streamNode, FIELD_ONE);
    const resultFieldTwo = getFieldChangeStatus(initialStream, streamNode, FIELD_TWO);

    expect(resultFieldOne).toBe("removed");
    expect(resultFieldTwo).toBe("unchanged");
  });

  // corner case - first field is deselected
  it("returns 'removed' when the first field is deselected but was initially selected", () => {
    const initialStream: AirbyteStreamAndConfiguration = {
      ...mockedInitialStream,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: false,
      },
    };

    const streamNode = {
      ...mockedStreamNode,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_THREE.path }], // field_two is removed
      },
    };

    const resultFieldOne = getFieldChangeStatus(initialStream, streamNode, FIELD_ONE);
    const resultFieldTwo = getFieldChangeStatus(initialStream, streamNode, FIELD_TWO);
    const resultFieldThree = getFieldChangeStatus(initialStream, streamNode, FIELD_THREE);

    expect(resultFieldOne).toBe("unchanged");
    expect(resultFieldTwo).toBe("removed");
    expect(resultFieldThree).toBe("unchanged");
  });

  // mix changes
  it("returns 'removed' for deselected  field, `added` for newly selected and `unchanged` for unchanged", () => {
    const initialStream: AirbyteStreamAndConfiguration = {
      ...mockedInitialStream,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      },
    };

    const streamNode = {
      ...mockedStreamNode,
      config: {
        ...mockedStreamNode.config!,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }], // field_one is removed and field_three is added
      },
    };

    const resultFieldOne = getFieldChangeStatus(initialStream, streamNode, FIELD_ONE);
    const resultFieldTwo = getFieldChangeStatus(initialStream, streamNode, FIELD_TWO);
    const resultFieldThree = getFieldChangeStatus(initialStream, streamNode, FIELD_THREE);

    expect(resultFieldOne).toBe("removed");
    expect(resultFieldTwo).toBe("unchanged");
    expect(resultFieldThree).toBe("added");
  });
});

/**
 * checkIsFieldHashed function tests
 */
describe(`${checkIsFieldHashed.name}`, () => {
  it("returns false when no mappers are present", () => {
    const config = { ...mockedStreamNode.config!, mappers: [] };
    const result = checkIsFieldHashed(FIELD_ONE, config);
    expect(result).toBe(false);
  });

  it("returns false when mappers are not defined", () => {
    const config = { ...mockedInitialStream.config! };
    const result = checkIsFieldHashed(FIELD_ONE, config);
    expect(result).toBe(false);
  });

  it("returns true when the field is referenced in a hashing mapper", () => {
    const config = {
      ...mockedInitialStream.config!,
      mappers: [
        {
          type: StreamMapperType.hashing,
          mapperConfiguration: {
            targetField: FIELD_ONE.path.join("."),
            method: HashingMapperConfigurationMethod.MD5,
            fieldNameSuffix: "_hashed",
          },
        },
      ],
    };
    const result = checkIsFieldHashed(FIELD_ONE, config);
    expect(result).toBe(true);
  });

  it("returns false when the field is not in the hashing mappers", () => {
    const config = {
      ...mockedInitialStream.config!,
      mappers: [
        {
          type: StreamMapperType.hashing,
          mapperConfiguration: {
            targetField: FIELD_TWO.path.join("."),
            method: HashingMapperConfigurationMethod.MD5,
            fieldNameSuffix: "_hashed",
          },
        },
      ],
    };
    const result = checkIsFieldHashed(FIELD_ONE, config);
    expect(result).toBe(false);
  });
});
