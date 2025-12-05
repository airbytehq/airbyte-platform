import { mockAirbyteStream } from "test-utils/mock-data/mockAirbyteStream";
import { mockStreamConfiguration } from "test-utils/mock-data/mockAirbyteStreamConfiguration";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { SyncSchemaField } from "core/domain/catalog";

import { mergeFieldPathArrays } from "./miscUtils";
import {
  updatePrimaryKey,
  updateCursorField,
  updateFieldSelected,
  getSelectedMandatoryFields,
  updateStreamSyncMode,
  updateFieldHashing,
} from "./streamConfigHelpers";
import { SyncModeValue } from "../components/SyncModeCell";

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
const FIELD_THREE: SyncSchemaField = {
  cleanedName: "field_three",
  type: "todo",
  key: "field_three",
  path: ["field_three"],
};

const mockSyncSchemaFields: SyncSchemaField[] = [FIELD_ONE, FIELD_TWO, FIELD_THREE];

describe(`${mergeFieldPathArrays.name}`, () => {
  it("merges two arrays of fieldPaths without duplicates", () => {
    const arr1 = [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }];
    const arr2 = [{ fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }];

    expect(mergeFieldPathArrays(arr1, arr2)).toEqual([
      { fieldPath: FIELD_ONE.path },
      { fieldPath: FIELD_TWO.path },
      { fieldPath: FIELD_THREE.path },
    ]);
  });

  it("merges two arrays of complex fieldPaths without duplicates", () => {
    const arr1 = [
      { fieldPath: [...FIELD_ONE.path, ...FIELD_TWO.path] },
      { fieldPath: [...FIELD_TWO.path, ...FIELD_THREE.path] },
    ];
    const arr2 = [
      { fieldPath: [...FIELD_ONE.path, ...FIELD_TWO.path] },
      { fieldPath: [...FIELD_TWO.path, ...FIELD_THREE.path] },
      { fieldPath: [...FIELD_ONE.path, ...FIELD_THREE.path] },
    ];

    expect(mergeFieldPathArrays(arr1, arr2)).toEqual([
      { fieldPath: [...FIELD_ONE.path, ...FIELD_TWO.path] },
      { fieldPath: [...FIELD_TWO.path, ...FIELD_THREE.path] },
      { fieldPath: [...FIELD_ONE.path, ...FIELD_THREE.path] },
    ]);
  });
});

describe(`${updateCursorField.name}`, () => {
  it("updates the cursor field when field selection is disabled", () => {
    const mockConfig: AirbyteStreamConfiguration = {
      ...mockStreamConfiguration,
      fieldSelectionEnabled: false,
      selectedFields: [],
    };

    const newStreamConfiguration = updateCursorField(mockConfig, FIELD_ONE.path, 3);

    expect(newStreamConfiguration).toEqual({
      cursorField: FIELD_ONE.path,
    });
  });
  describe("when fieldSelection is active", () => {
    it("adds the cursor to selectedFields", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updateCursorField(mockConfig, FIELD_THREE.path, 100);

      expect(newStreamConfiguration).toEqual({
        cursorField: FIELD_THREE.path,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
      });
    });

    it("updates the cursor field when only one other field is unselected", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updateCursorField(mockConfig, FIELD_ONE.path, 3);

      expect(newStreamConfiguration).toEqual({
        cursorField: FIELD_ONE.path,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      });
    });

    it("updates the cursor field when it is one of many unselected fields", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updateCursorField(mockConfig, ["new_cursor"], 100);

      expect(newStreamConfiguration).toEqual({
        cursorField: ["new_cursor"],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: ["new_cursor"] }],
      });
    });

    it("disables field selection when the selected cursor is the only unselected field", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updateCursorField(mockConfig, FIELD_THREE.path, 3);

      expect(newStreamConfiguration).toEqual({
        cursorField: FIELD_THREE.path,
        fieldSelectionEnabled: false,
        selectedFields: [],
      });
    });
  });
});

describe(`${updatePrimaryKey.name}`, () => {
  it("updates the primary key field", () => {
    const mockConfig: AirbyteStreamConfiguration = {
      ...mockStreamConfiguration,
      primaryKey: [FIELD_ONE.path],
    };

    const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_TWO.path], 3);

    expect(newStreamConfiguration).toEqual({
      primaryKey: [FIELD_TWO.path],
    });
  });

  describe("when fieldSelection is active", () => {
    it("adds each piece of the composite primary key to selectedFields", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        primaryKey: [FIELD_ONE.path],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }],
      };

      const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_TWO.path, FIELD_THREE.path], 100);

      expect(newStreamConfiguration).toEqual({
        primaryKey: [FIELD_TWO.path, FIELD_THREE.path],
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
        fieldSelectionEnabled: true,
      });
    });

    it("replaces the primary key when many other field are unselected", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        primaryKey: [],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_THREE.path], 100);

      expect(newStreamConfiguration).toEqual({
        primaryKey: [FIELD_THREE.path],
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
        fieldSelectionEnabled: true,
      });
    });

    it("replaces the primary key when only one other field is unselected", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        primaryKey: [FIELD_ONE.path],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_TWO.path], 3);

      expect(newStreamConfiguration).toEqual({
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
        primaryKey: [FIELD_TWO.path],
      });
    });

    it("disables field selection when the selected primary key is the last unselected field", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        primaryKey: [],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }],
      };

      const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_TWO.path, FIELD_THREE.path], 3);

      expect(newStreamConfiguration).toEqual({
        primaryKey: [FIELD_TWO.path, FIELD_THREE.path],
        selectedFields: [],
        fieldSelectionEnabled: false,
      });
    });

    it("disables field selection when part of the selected primary key is the last unselected field", () => {
      const mockConfig: AirbyteStreamConfiguration = {
        ...mockStreamConfiguration,
        primaryKey: [FIELD_ONE.path],
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      };

      const newStreamConfiguration = updatePrimaryKey(mockConfig, [FIELD_THREE.path], 3);

      expect(newStreamConfiguration).toEqual({
        primaryKey: [FIELD_THREE.path],
        selectedFields: [],
        fieldSelectionEnabled: false,
      });
    });
  });
});

describe(`${updateFieldSelected.name}`, () => {
  it("Adds a field to selectedFields when selected", () => {
    const newStreamConfiguration = updateFieldSelected({
      config: {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      },
      fieldPath: FIELD_THREE.path,
      isSelected: true,
      numberOfFieldsInStream: 5,
      fields: mockSyncSchemaFields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
    });
  });

  it("Removes a field to selectedFields when deselected selected", () => {
    const newStreamConfiguration = updateFieldSelected({
      config: {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
      },
      fieldPath: FIELD_THREE.path,
      isSelected: false,
      numberOfFieldsInStream: 5,
      fields: mockSyncSchemaFields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
    });
  });

  it("Deselects the first field, enabling fieldSelection", () => {
    const newStreamConfiguration = updateFieldSelected({
      config: mockStreamConfiguration,
      fieldPath: FIELD_ONE.path,
      isSelected: false,
      numberOfFieldsInStream: 3,
      fields: mockSyncSchemaFields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: FIELD_TWO.path }, { fieldPath: FIELD_THREE.path }],
    });
  });

  it("Selects the last unselected field", () => {
    const newStreamConfiguration = updateFieldSelected({
      config: {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
      },
      fieldPath: FIELD_THREE.path,
      isSelected: true,
      numberOfFieldsInStream: 3,
      fields: mockSyncSchemaFields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: false,
      selectedFields: [],
    });
  });

  it("auto-selects all CDC fields when selecting a regular field", () => {
    const CDC_CURSOR: SyncSchemaField = {
      path: ["_ab_cdc_cursor"],
      cleanedName: "_ab_cdc_cursor",
      key: "_ab_cdc_cursor",
      type: "string",
    };

    const CDC_DELETED: SyncSchemaField = {
      path: ["_ab_cdc_deleted_at"],
      cleanedName: "_ab_cdc_deleted_at",
      key: "_ab_cdc_deleted_at",
      type: "string",
    };

    const REGULAR_FIELD: SyncSchemaField = {
      path: ["regular_field"],
      cleanedName: "regular_field",
      key: "regular_field",
      type: "string",
    };

    const fields = [CDC_CURSOR, CDC_DELETED, REGULAR_FIELD, FIELD_ONE];

    const newStreamConfiguration = updateFieldSelected({
      config: {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [],
      },
      fieldPath: REGULAR_FIELD.path,
      isSelected: true,
      numberOfFieldsInStream: 4,
      fields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: true,
      selectedFields: [
        { fieldPath: REGULAR_FIELD.path },
        { fieldPath: CDC_CURSOR.path },
        { fieldPath: CDC_DELETED.path },
      ],
    });
  });

  it("does not auto-select CDC fields when no CDC fields exist in the stream", () => {
    const REGULAR_FIELD: SyncSchemaField = {
      path: ["regular_field"],
      cleanedName: "regular_field",
      key: "regular_field",
      type: "string",
    };

    const fields = [REGULAR_FIELD, FIELD_ONE, FIELD_TWO];

    const newStreamConfiguration = updateFieldSelected({
      config: {
        ...mockStreamConfiguration,
        fieldSelectionEnabled: true,
        selectedFields: [],
      },
      fieldPath: REGULAR_FIELD.path,
      isSelected: true,
      numberOfFieldsInStream: 3,
      fields,
    });

    expect(newStreamConfiguration).toEqual({
      fieldSelectionEnabled: true,
      selectedFields: [{ fieldPath: REGULAR_FIELD.path }],
    });
  });
});

describe(`${getSelectedMandatoryFields.name}`, () => {
  it("returns an empty array if the stream selected(enabled)", () => {
    const mandatoryFields = getSelectedMandatoryFields({ ...mockStreamConfiguration, selected: true });
    expect(mandatoryFields).toEqual([]);
  });

  it("returns an empty array if sync mode is full_refresh", () => {
    const mandatoryFields = getSelectedMandatoryFields({
      ...mockStreamConfiguration,
      selected: false,
      primaryKey: [FIELD_ONE.path, FIELD_TWO.path],
      cursorField: FIELD_ONE.path,
      syncMode: "full_refresh",
    });

    expect(mandatoryFields).toEqual([]);
  });

  it("returns the primary key fields if the destinationSyncMode is append_dedup", () => {
    const mandatoryFields = getSelectedMandatoryFields({
      ...mockStreamConfiguration,
      selected: false,
      primaryKey: [FIELD_ONE.path, FIELD_TWO.path],
      destinationSyncMode: "append_dedup",
    });

    expect(mandatoryFields).toEqual([{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }]);
  });

  it("returns the primary key fields if the destinationSyncMode is overwrite_dedup", () => {
    const mandatoryFields = getSelectedMandatoryFields({
      ...mockStreamConfiguration,
      selected: false,
      primaryKey: [FIELD_ONE.path, FIELD_TWO.path],
      destinationSyncMode: "overwrite_dedup",
    });

    expect(mandatoryFields).toEqual([{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }]);
  });

  it("returns the cursor field if the syncMode is incremental", () => {
    const mandatoryFields = getSelectedMandatoryFields({
      ...mockStreamConfiguration,
      selected: false,
      cursorField: FIELD_ONE.path,
      syncMode: "incremental",
    });

    expect(mandatoryFields).toEqual([{ fieldPath: FIELD_ONE.path }]);
  });
});

describe(`${updateStreamSyncMode.name}`, () => {
  it("updates the sync modes", () => {
    const syncModes: SyncModeValue = {
      syncMode: "full_refresh",
      destinationSyncMode: "overwrite",
    };
    expect(updateStreamSyncMode(mockAirbyteStream, mockStreamConfiguration, syncModes)).toEqual(
      expect.objectContaining({ ...syncModes })
    );
  });

  describe("when fieldSelection is enabled", () => {
    const PK_PART_ONE = ["pk_part_one"];
    const PK_PART_TWO = ["pk_part_two"];
    const DEFAULT_CURSOR_FIELD_PATH = ["default_cursor"];
    const DEFAULT_PRIMARY_KEY = [PK_PART_ONE, PK_PART_TWO];
    const UNRELATED_FIELD_PATH = ["unrelated_field_path"];

    it("does not add default pk or cursor for irrelevant sync modes", () => {
      const syncModes: SyncModeValue = {
        syncMode: "full_refresh",
        destinationSyncMode: "overwrite",
      };
      const updatedConfig = updateStreamSyncMode(
        {
          ...mockAirbyteStream,
          sourceDefinedCursor: true,
          defaultCursorField: DEFAULT_CURSOR_FIELD_PATH,
          sourceDefinedPrimaryKey: DEFAULT_PRIMARY_KEY,
        },
        mockStreamConfiguration,
        syncModes
      );

      expect(updatedConfig).toEqual(
        expect.objectContaining({
          fieldSelectionEnabled: false,
          selectedFields: [],
          ...syncModes,
        })
      );
    });

    it("automatically selects the default cursor", () => {
      const syncModes: SyncModeValue = {
        syncMode: "incremental",
        destinationSyncMode: "append",
      };

      const updatedConfig = updateStreamSyncMode(
        { ...mockAirbyteStream, sourceDefinedCursor: true, defaultCursorField: DEFAULT_CURSOR_FIELD_PATH },
        {
          ...mockStreamConfiguration,
          fieldSelectionEnabled: true,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }],
        },
        syncModes
      );

      expect(updatedConfig).toEqual(
        expect.objectContaining({
          ...syncModes,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }, { fieldPath: DEFAULT_CURSOR_FIELD_PATH }],
        })
      );
    });

    it("automatically selects the composite primary key fields (append)", () => {
      const syncModes: SyncModeValue = {
        syncMode: "incremental",
        destinationSyncMode: "append_dedup",
      };

      const updatedConfig = updateStreamSyncMode(
        { ...mockAirbyteStream, sourceDefinedPrimaryKey: DEFAULT_PRIMARY_KEY },
        {
          ...mockStreamConfiguration,
          fieldSelectionEnabled: true,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }],
        },
        syncModes
      );

      expect(updatedConfig).toEqual(
        expect.objectContaining({
          ...syncModes,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }, { fieldPath: PK_PART_ONE }, { fieldPath: PK_PART_TWO }],
        })
      );
    });

    it("automatically selects the composite primary key fields (overwrite)", () => {
      const syncModes: SyncModeValue = {
        syncMode: "incremental",
        destinationSyncMode: "overwrite_dedup",
      };

      const updatedConfig = updateStreamSyncMode(
        { ...mockAirbyteStream, sourceDefinedPrimaryKey: DEFAULT_PRIMARY_KEY },
        {
          ...mockStreamConfiguration,
          fieldSelectionEnabled: true,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }],
        },
        syncModes
      );

      expect(updatedConfig).toEqual(
        expect.objectContaining({
          ...syncModes,
          selectedFields: [{ fieldPath: UNRELATED_FIELD_PATH }, { fieldPath: PK_PART_ONE }, { fieldPath: PK_PART_TWO }],
        })
      );
    });
  });
});

describe(`${updateFieldHashing.name}`, () => {
  it("adds a field to hashedFields when isFieldHashed is true", () => {
    const mockConfig: AirbyteStreamConfiguration = {
      ...mockStreamConfiguration,
      hashedFields: [{ fieldPath: FIELD_ONE.path }],
    };

    const newStreamConfiguration = updateFieldHashing({
      config: mockConfig,
      fieldPath: FIELD_TWO.path,
      isFieldHashed: true,
    });

    expect(newStreamConfiguration).toEqual({
      hashedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
    });
  });

  it("removes a field from hashedFields when isFieldHashed is false", () => {
    const mockConfig: AirbyteStreamConfiguration = {
      ...mockStreamConfiguration,
      hashedFields: [{ fieldPath: FIELD_ONE.path }, { fieldPath: FIELD_TWO.path }],
    };

    const newStreamConfiguration = updateFieldHashing({
      config: mockConfig,
      fieldPath: FIELD_TWO.path,
      isFieldHashed: false,
    });

    expect(newStreamConfiguration).toEqual({
      hashedFields: [{ fieldPath: FIELD_ONE.path }],
    });
  });

  it("removes the last field from hashedFields and sets hashedFields to undefined", () => {
    const mockConfig: AirbyteStreamConfiguration = {
      ...mockStreamConfiguration,
      hashedFields: [{ fieldPath: FIELD_ONE.path }],
    };

    const newStreamConfiguration = updateFieldHashing({
      config: mockConfig,
      fieldPath: FIELD_ONE.path,
      isFieldHashed: false,
    });

    expect(newStreamConfiguration).toEqual({
      hashedFields: undefined,
    });
  });
});
