import { Row } from "@tanstack/react-table";

import { AirbyteStreamAndConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import { SyncCatalogUIModel } from "./SyncCatalogTable";
import { generateTestId, getFieldChangeStatus, getStreamChangeStatus } from "./utils";
import { SyncStreamFieldWithId } from "../formConfig";

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
