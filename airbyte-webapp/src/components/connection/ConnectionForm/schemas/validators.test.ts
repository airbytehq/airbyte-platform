import { z } from "zod";

import { AirbyteStreamAndConfiguration, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import {
  atLeastOneStreamSelectedValidation,
  streamConfigurationValidation,
  hashFieldCollisionValidation,
} from "./validators";

describe("atLeastOneStreamSelectedValidation", () => {
  const mockRefinementCtx = {
    addIssue: jest.fn(),
  } as unknown as z.RefinementCtx;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should return true when at least one stream is selected", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
        stream: {
          name: "test-stream",
        },
      },
    ];

    const result = atLeastOneStreamSelectedValidation(streams, mockRefinementCtx);

    expect(result).toBe(true);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should return true when at least one stream is selected among multiple streams", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        config: {
          selected: false,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
        stream: {
          name: "test-stream-1",
        },
      },
      {
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
        stream: {
          name: "test-stream-2",
        },
      },
      {
        config: {
          selected: false,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
        stream: {
          name: "test-stream-3",
        },
      },
    ];

    const result = atLeastOneStreamSelectedValidation(streams, mockRefinementCtx);

    expect(result).toBe(true);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should return false and add an issue when no streams are selected", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        config: {
          selected: false,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
        stream: {
          name: "test-stream",
        },
      },
    ];

    const result = atLeastOneStreamSelectedValidation(streams, mockRefinementCtx);

    expect(result).toBe(false);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.required",
      fatal: true,
    });
  });

  it("should return false and add an issue when streams array is empty", () => {
    const streams: AirbyteStreamAndConfiguration[] = [];

    const result = atLeastOneStreamSelectedValidation(streams, mockRefinementCtx);

    expect(result).toBe(false);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.required",
      fatal: true,
    });
  });

  it("should handle undefined config gracefully", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
        },
      },
    ];

    const result = atLeastOneStreamSelectedValidation(streams, mockRefinementCtx);

    expect(result).toBe(false);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.required",
      fatal: true,
    });
  });
});

describe("streamConfigurationValidation", () => {
  const mockRefinementCtx = {
    addIssue: jest.fn(),
  } as unknown as z.RefinementCtx;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should not add issues for valid stream configurations", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          primaryKey: [["id"]],
          cursorField: ["updated_at"],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should skip validation for non-selected streams", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        config: {
          selected: false,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [],
          cursorField: [],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should add an issue when primary key is missing for append_dedup mode", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.primaryKey.required",
      path: ["test-stream_test-namespace.config.primaryKey"],
    });
  });

  it("should add an issue when primary key is missing for overwrite_dedup mode", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.overwrite_dedup,
          primaryKey: [],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.primaryKey.required",
      path: ["test-stream_test-namespace.config.primaryKey"],
    });
  });

  it("should not add an issue when primary key is provided for dedup modes", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [["id"]],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should add an issue when cursor field is missing for incremental mode", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          sourceDefinedCursor: false,
        },
        config: {
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: [],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.cursorField.required",
      path: ["test-stream_test-namespace.config.cursorField"],
    });
  });

  it("should not add an issue when cursor field is provided for incremental mode", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          sourceDefinedCursor: false,
        },
        config: {
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: ["updated_at"],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should not add an issue when source defined cursor is true for incremental mode", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          sourceDefinedCursor: true,
        },
        config: {
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: [],
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should handle empty cursorField array with non-empty values", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          sourceDefinedCursor: false,
        },
        config: {
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: ["", ""], // Empty strings should be filtered out
        },
      },
    ];

    streamConfigurationValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.cursorField.required",
      path: ["test-stream_test-namespace.config.cursorField"],
    });
  });

  it("should handle undefined stream or config gracefully", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        // Missing stream
        config: {
          selected: true,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: [],
        },
      },
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
        },
        // Missing config
      },
    ];

    // Should not throw an error
    expect(() => streamConfigurationValidation(streams, mockRefinementCtx)).not.toThrow();
  });
});

describe("hashFieldCollisionValidation", () => {
  const mockRefinementCtx = {
    addIssue: jest.fn(),
  } as unknown as z.RefinementCtx;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should not add issues when there are no hash field collisions", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["other_field"] }],
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should not add issues when streams are not selected", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: false,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["id"] }],
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should not add issues when streams don't have hashed fields", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should add an issue when there is a hash field collision", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              id_hashed: { type: "string" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["id"] }],
          selectedFields: [],
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.hashFieldCollision",
    });
  });

  it("should handle undefined streams gracefully", () => {
    const streams: AirbyteStreamAndConfiguration[] = undefined as unknown as AirbyteStreamAndConfiguration[];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should handle empty streams array gracefully", () => {
    const streams: AirbyteStreamAndConfiguration[] = [];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should handle streams with selectedFields properly", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["id"] }],
          selectedFields: [{ fieldPath: ["name"] }], // Only name is selected, id is not
          fieldSelectionEnabled: true,
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    // Since id is not in selectedFields, there should be no collision
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should not detect collision when only the field exists without _hashed suffix", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["id"] }],
          selectedFields: [],
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    // No collision when only 'id' exists without 'id_hashed'
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should not detect collision when only the field_hashed exists", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id_hashed: { type: "string" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["non_existent_field"] }],
          selectedFields: [],
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    // No collision when only 'id_hashed' exists without 'id'
    expect(mockRefinementCtx.addIssue).not.toHaveBeenCalled();
  });

  it("should detect collision with explicit field selection", () => {
    const streams: AirbyteStreamAndConfiguration[] = [
      {
        stream: {
          name: "test-stream",
          namespace: "test-namespace",
          jsonSchema: {
            type: "object",
            properties: {
              id: { type: "integer" },
              id_hashed: { type: "string" },
              name: { type: "string" },
            },
          },
        },
        config: {
          selected: true,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          hashedFields: [{ fieldPath: ["id"] }],
          selectedFields: [{ fieldPath: ["id"] }, { fieldPath: ["id_hashed"] }],
          fieldSelectionEnabled: true,
        },
      },
    ];

    hashFieldCollisionValidation(streams, mockRefinementCtx);
    expect(mockRefinementCtx.addIssue).toHaveBeenCalledWith({
      code: z.ZodIssueCode.custom,
      message: "connectionForm.streams.hashFieldCollision",
    });
  });
});
