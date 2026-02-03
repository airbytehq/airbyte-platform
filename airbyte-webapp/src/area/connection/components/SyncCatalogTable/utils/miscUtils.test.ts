import { Row } from "@tanstack/react-table";

import { compareObjectsByFields, generateTestId } from "./miscUtils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

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
