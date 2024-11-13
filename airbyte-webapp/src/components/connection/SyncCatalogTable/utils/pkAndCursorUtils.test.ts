import { mockStreamConfiguration } from "test-utils/mock-data/mockAirbyteStreamConfiguration";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";

import {
  isCursor,
  isChildFieldCursor,
  isPrimaryKey,
  isChildFieldPrimaryKey,
  checkCursorAndPKRequirements,
} from "./pkAndCursorUtils";

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

describe(`${checkCursorAndPKRequirements.name}`, () => {
  const mockStream = {
    name: "my_stream",
    sourceDefinedPrimaryKey: [],
    sourceDefinedCursor: false,
  };

  it("returns correct requirements when pk and cursor are required", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      syncMode: "incremental",
      destinationSyncMode: "append_dedup",
    };
    const result = checkCursorAndPKRequirements(config, mockStream);
    expect(result).toEqual({
      pkRequired: true,
      cursorRequired: true,
      shouldDefinePk: true,
      shouldDefineCursor: true,
    });
  });

  it("returns correct requirements when only pk is required", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      syncMode: "full_refresh",
      destinationSyncMode: "append_dedup",
    };
    const result = checkCursorAndPKRequirements(config, mockStream);
    expect(result).toEqual({
      pkRequired: true,
      cursorRequired: false,
      shouldDefinePk: true,
      shouldDefineCursor: false,
    });
  });

  it("returns correct requirements when only cursor is required", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      syncMode: "incremental",
      destinationSyncMode: "overwrite",
    };
    const result = checkCursorAndPKRequirements(config, mockStream);
    expect(result).toEqual({
      pkRequired: false,
      cursorRequired: true,
      shouldDefinePk: false,
      shouldDefineCursor: true,
    });
  });

  it("returns correct requirements when neither pk nor cursor are required", () => {
    const config: AirbyteStreamConfiguration = {
      ...mockIncrementalDedupConfig,
      syncMode: "full_refresh",
      destinationSyncMode: "overwrite",
    };
    const result = checkCursorAndPKRequirements(config, mockStream);
    expect(result).toEqual({
      pkRequired: false,
      cursorRequired: false,
      shouldDefinePk: false,
      shouldDefineCursor: false,
    });
  });
});
