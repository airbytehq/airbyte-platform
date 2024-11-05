import { Row } from "@tanstack/react-table";

import {
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import {
  compareObjectsByFields,
  generateTestId,
  getFieldChangeStatus,
  getStreamChangeStatus,
  isChildFieldCursor,
  isChildFieldPrimaryKey,
  isCursor,
  isPrimaryKey,
} from "./utils";
import { mockStreamConfiguration } from "../../../../test-utils/mock-data/mockAirbyteStreamConfiguration";
import { SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

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
 * getStreamChangeStatus function tests
 */
describe(`${getStreamChangeStatus.name}`, () => {
  it("should return 'unchanged' status", () => {
    const result = getStreamChangeStatus(mockedInitialStream, mockedStreamNode);
    expect(result).toEqual("unchanged");
  });

  it("should return 'disabled' status for a row that initially was not enabled", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      {
        ...mockedStreamNode,
        config: { ...mockedStreamNode.config!, selected: false },
      }
    );
    expect(result).toEqual("disabled");
  });

  it("should return 'added' status for a row that initially was disabled", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      mockedStreamNode
    );
    expect(result).toEqual("added");
  });

  it("should return 'removed' status for a row that initially was enabled", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, selected: false },
    });
    expect(result).toEqual("removed");
  });

  it("should return 'updated' status for a row that has changed 'syncMode' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, syncMode: SyncMode.full_refresh },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'destinationSyncMode' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, destinationSyncMode: DestinationSyncMode.append },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'cursorField' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, cursorField: ["create_time"] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'primaryKey' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, primaryKey: [["id"]] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'selectedFields' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, selectedFields: [{ fieldPath: FIELD_ONE.path }] },
    });
    expect(result).toEqual("changed");
  });

  it("should return 'updated' status for a row that has changed 'fieldSelectionEnabled' prop", () => {
    const result = getStreamChangeStatus(mockedInitialStream, {
      ...mockedStreamNode,
      config: { ...mockedStreamNode.config!, fieldSelectionEnabled: false },
    });
    expect(result).toEqual("changed");
  });

  it("should return added styles for a row that is both added and updated", () => {
    const result = getStreamChangeStatus(
      { ...mockedInitialStream, config: { ...mockedInitialStream.config!, selected: false } },
      {
        ...mockedStreamNode,
        config: { ...mockedStreamNode.config!, syncMode: SyncMode.full_refresh }, // selected true, new sync mode
      }
    );
    expect(result).toEqual("added");
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

describe(`${generateTestId.name}`, () => {
  it("returns correct test id for namespace row", () => {
    const row = { original: { rowType: "namespace", name: "public" }, depth: 0 } as Row<SyncCatalogUIModel>;
    const result = generateTestId(row);
    expect(result).toBe("row-depth-0-namespace-public");
  });

  it("returns correct test id for namespace row with no name", () => {
    const row = { original: { rowType: "namespace", name: "" }, depth: 0 } as Row<SyncCatalogUIModel>;
    const result = generateTestId(row);
    expect(result).toBe("row-depth-0-namespace-no-name");
  });

  it("returns correct test id for stream row", () => {
    const row = { original: { rowType: "stream", name: "activities" }, depth: 1 } as Row<SyncCatalogUIModel>;
    const result = generateTestId(row);
    expect(result).toBe("row-depth-1-stream-activities");
  });

  it("returns correct test id for field row", () => {
    const row = { original: { rowType: "field", name: "id" }, depth: 2 } as Row<SyncCatalogUIModel>;
    const result = generateTestId(row);
    expect(result).toBe("row-depth-2-field-id");
  });

  it("returns unknown type for unrecognized row type", () => {
    const row = { original: { rowType: "nestedField", name: "id" }, depth: 3 } as Row<SyncCatalogUIModel>;
    const result = generateTestId(row);
    expect(result).toBe("row-unknown-type");
  });
});

const mockIncrementalConfig: AirbyteStreamConfiguration = {
  ...mockStreamConfiguration,
  syncMode: "incremental",
};

const mockIncrementalDedupConfig: AirbyteStreamConfiguration = {
  ...mockStreamConfiguration,
  syncMode: "incremental",
  destinationSyncMode: "append_dedup",
};

describe(`${isCursor.name}`, () => {
  it("returns true if the path matches the cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: ["my_cursor"],
    };
    expect(isCursor(config, ["my_cursor"])).toBe(true);
  });

  it("returns false if there is no cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: undefined,
    };
    expect(isCursor(config, ["my_cursor"])).toBe(false);
  });

  it("returns false if the path does not match the cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: ["my_cursor"],
    };
    expect(isCursor(config, ["some_other_field"])).toBe(false);
  });
});

describe(`${isChildFieldCursor.name}`, () => {
  it("returns true if the path is the parent of the cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: ["my_cursor", "nested_field"],
    };
    expect(isChildFieldCursor(config, ["my_cursor"])).toBe(true);
  });

  it("returns false if there is no cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: undefined,
    };
    expect(isChildFieldCursor(config, ["my_cursor"])).toBe(false);
  });

  it("returns false if the path does not match the cursor", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalConfig,
      cursorField: ["my_cursor", "nested_field"],
    };
    expect(isChildFieldCursor(config, ["some_other_field"])).toBe(false);
  });
});

describe(`${isPrimaryKey.name}`, () => {
  it("returns true if the path matches any part of the primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: [["some_other_pk"], ["my_pk"]],
    };
    expect(isPrimaryKey(config, ["my_pk"])).toBe(true);
  });

  it("returns false if there is no primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: undefined,
    };
    expect(isPrimaryKey(config, ["my_pk"])).toBe(false);
  });

  it("returns false if the path does not match any part of the primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: [["some_other_pk"], ["my_pk"]],
    };
    expect(isPrimaryKey(config, ["unrelated_field"])).toBe(false);
  });
});

describe(`${isChildFieldPrimaryKey.name}`, () => {
  it("returns true if the path is the parent of any part of the primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: [
        ["some_other_pk", "nested_field"],
        ["my_pk", "nested_field"],
      ],
    };
    expect(isChildFieldPrimaryKey(config, ["my_pk"])).toBe(true);
  });

  it("returns false if there is no primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: undefined,
    };
    expect(isChildFieldPrimaryKey(config, ["my_pk"])).toBe(false);
  });

  it("returns false if the path is not the parent of any part of the primary key", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      primaryKey: [
        ["some_other_pk", "nested_field"],
        ["my_pk", "nested_field"],
      ],
    };
    expect(isChildFieldPrimaryKey(config, ["unrelated_field"])).toBe(false);
  });
});

describe(`${compareObjectsByFields.name}`, () => {
  it("should return true for equal objects by the given props", () => {
    const obj1 = {
      id: 1,
      name: "John Doe",
      emails: ["john@example.com", "doe@example.com"],
      age: 30,
    };

    const obj2 = {
      id: 1,
      name: "John Doe",
      emails: ["john@example.com", "doe@example.com"],
      age: 30,
    };

    const fieldsToCompare: Array<keyof typeof obj1> = ["id", "name", "emails", "age"];

    const result = compareObjectsByFields(obj1, obj2, fieldsToCompare);
    expect(result).toBe(true);
  });

  it("should return false for different objects by the given props", () => {
    const obj1 = {
      id: 1,
      name: "John Doe",
      age: 30,
    };

    const obj2 = {
      id: 1,
      name: "Jane Smith",
      age: 25,
    };

    const fieldsToCompare: Array<keyof typeof obj1> = ["id", "name", "age"];

    const result = compareObjectsByFields(obj1, obj2, fieldsToCompare);
    expect(result).toBe(false);
  });

  it("should return false if any object is undefined", () => {
    const obj1 = {
      id: 1,
      name: "John Doe",
      age: 30,
    };

    const obj2 = undefined;

    const fieldsToCompare: Array<keyof typeof obj1> = ["id", "name", "age"];

    const result = compareObjectsByFields(obj1, obj2, fieldsToCompare);
    expect(result).toBe(false);
  });

  it("should return true for equal objects with nested arrays", () => {
    const obj1 = {
      id: 1,
      name: "John Doe",
      emails: ["john@example.com", "doe@example.com"],
      age: 30,
    };

    const obj2 = {
      id: 1,
      name: "John Doe",
      emails: ["doe@example.com", "john@example.com"],
      age: 30,
    };

    const fieldsToCompare: Array<keyof typeof obj1> = ["id", "name", "emails", "age"];

    const result = compareObjectsByFields(obj1, obj2, fieldsToCompare);
    expect(result).toBe(true);
  });

  it("should return true for objects with nested arrays of objects if they are even in different order", () => {
    const obj1 = {
      syncMode: "incremental",
      cursorField: ["create_time"],
      destinationSyncMode: "append_dedup",
      primaryKey: [["id"]],
      aliasName: "automations",
      selected: true,
      fieldSelectionEnabled: true,
      selectedFields: [
        { fieldPath: ["_links"] },
        { fieldPath: ["trigger_settings"] },
        { fieldPath: ["id"] },
        { fieldPath: ["tracking"] },
        { fieldPath: ["status"] },
      ],
    };

    const obj2 = {
      syncMode: "incremental",
      cursorField: ["create_time"],
      destinationSyncMode: "append_dedup",
      primaryKey: [["id"]],
      aliasName: "automations",
      selected: true,
      fieldSelectionEnabled: true,
      selectedFields: [
        { fieldPath: ["status"] },
        { fieldPath: ["_links"] },
        { fieldPath: ["trigger_settings"] },
        { fieldPath: ["id"] },
        { fieldPath: ["tracking"] },
      ],
    };

    const fieldsToCompare: Array<keyof typeof obj1> = [
      "syncMode",
      "cursorField",
      "destinationSyncMode",
      "primaryKey",
      "aliasName",
      "selected",
      "fieldSelectionEnabled",
      "selectedFields",
    ];

    const result = compareObjectsByFields(obj1, obj2, fieldsToCompare);
    expect(result).toBe(true);
  });
});
