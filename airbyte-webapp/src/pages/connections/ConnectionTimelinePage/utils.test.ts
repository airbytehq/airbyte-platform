import {
  CatalogDiff,
  StreamTransform,
  StreamTransformTransformType,
  FieldTransformTransformType,
} from "core/api/types/AirbyteClient";

import { isSemanticVersionTags, isVersionUpgraded, transformCatalogDiffToCatalogConfigDiff } from "./utils";

describe(`'${isSemanticVersionTags.name}'`, () => {
  it("should return true for valid semantic version tags", () => {
    expect(isSemanticVersionTags("2.0.0", "1.0.0")).toBe(true);
    expect(isSemanticVersionTags("3.0.0", "3.0.1")).toBe(true);
  });

  it("should return false for invalid semantic version tags", () => {
    expect(isSemanticVersionTags("1", "2.0")).toBe(false);
    expect(isSemanticVersionTags("1.0", "2.0.0")).toBe(false);
    expect(isSemanticVersionTags("1.0.0", "dev")).toBe(false);
    expect(isSemanticVersionTags("dev", "1.0.0")).toBe(false);
    expect(isSemanticVersionTags("1.0.0", "1.0.0-rc1")).toBe(false);
  });
});

describe(`'${isVersionUpgraded.name}'`, () => {
  it("should return true if newVersion is greater than oldVersion", () => {
    expect(isVersionUpgraded("2.0", "1.0")).toBe(true);
    expect(isVersionUpgraded("1.1", "1.0")).toBe(true);
    expect(isVersionUpgraded("1.0.1", "1.0.0")).toBe(true);
    expect(isVersionUpgraded("1.0.0.1", "1.0.0.0")).toBe(true);
  });

  it("should return false if oldVersion is less than or equal to newVersion", () => {
    expect(isVersionUpgraded("1.0", "2.0")).toBe(false);
    expect(isVersionUpgraded("1.0", "1.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0", "1.0.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0.0", "1.0.0.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0", "1.0.0")).toBe(false);
  });
});

describe("transformCatalogDiffToCatalogConfigDiff", () => {
  const removedStreams: StreamTransform[] = [
    {
      transformType: StreamTransformTransformType.remove_stream,
      streamDescriptor: { namespace: "apple", name: "dragonfruit" },
    },
    {
      transformType: StreamTransformTransformType.remove_stream,
      streamDescriptor: { namespace: "apple", name: "eclair" },
    },
  ];

  const addedStreams: StreamTransform[] = [
    {
      transformType: StreamTransformTransformType.add_stream,
      streamDescriptor: { namespace: "apple", name: "banana" },
    },
    {
      transformType: StreamTransformTransformType.add_stream,
      streamDescriptor: { namespace: "apple", name: "carrot" },
    },
  ];

  const updatedStreams: StreamTransform[] = [
    {
      transformType: StreamTransformTransformType.update_stream,
      streamDescriptor: { namespace: "apple", name: "harissa_paste" },
      updateStream: {
        streamAttributeTransforms: [],
        fieldTransforms: [
          { transformType: FieldTransformTransformType.add_field, fieldName: ["users", "phone"], breaking: false },
          { transformType: FieldTransformTransformType.add_field, fieldName: ["users", "email"], breaking: false },
          {
            transformType: FieldTransformTransformType.remove_field,
            fieldName: ["users", "lastName"],
            breaking: false,
          },
          {
            transformType: FieldTransformTransformType.update_field_schema,
            fieldName: ["users", "address"],
            breaking: false,
            updateFieldSchema: { oldSchema: { type: "number" }, newSchema: { type: "string" } },
          },
        ],
      },
    },
  ];

  it("should handle stream additions and removals", () => {
    const catalogDiff: CatalogDiff = {
      transforms: [...addedStreams, ...removedStreams],
    };

    const result = transformCatalogDiffToCatalogConfigDiff(catalogDiff);

    expect(result.streamsAdded).toEqual([
      { streamName: "banana", streamNamespace: "apple" },
      { streamName: "carrot", streamNamespace: "apple" },
    ]);

    expect(result.streamsRemoved).toEqual([
      { streamName: "dragonfruit", streamNamespace: "apple" },
      { streamName: "eclair", streamNamespace: "apple" },
    ]);

    expect(result.fieldsAdded).toEqual([]);
    expect(result.fieldsRemoved).toEqual([]);
  });

  it("should handle field additions and removals within streams", () => {
    const catalogDiff: CatalogDiff = {
      transforms: updatedStreams,
    };

    const result = transformCatalogDiffToCatalogConfigDiff(catalogDiff);

    expect(result.streamsAdded).toEqual([]);
    expect(result.streamsRemoved).toEqual([]);

    expect(result.fieldsAdded).toEqual([
      {
        streamName: "harissa_paste",
        streamNamespace: "apple",
        fields: ["users.phone", "users.email"],
      },
    ]);

    expect(result.fieldsRemoved).toEqual([
      {
        streamName: "harissa_paste",
        streamNamespace: "apple",
        fields: ["users.lastName"],
      },
    ]);
  });

  it("should handle empty transforms", () => {
    const catalogDiff: CatalogDiff = {
      transforms: [],
    };

    const result = transformCatalogDiffToCatalogConfigDiff(catalogDiff);

    expect(result.streamsAdded).toEqual([]);
    expect(result.streamsRemoved).toEqual([]);
    expect(result.fieldsAdded).toEqual([]);
    expect(result.fieldsRemoved).toEqual([]);
  });

  it("should handle all types of changes together", () => {
    const catalogDiff: CatalogDiff = {
      transforms: [...addedStreams, ...removedStreams, ...updatedStreams],
    };

    const result = transformCatalogDiffToCatalogConfigDiff(catalogDiff);

    expect(result.streamsAdded).toEqual([
      { streamName: "banana", streamNamespace: "apple" },
      { streamName: "carrot", streamNamespace: "apple" },
    ]);

    expect(result.streamsRemoved).toEqual([
      { streamName: "dragonfruit", streamNamespace: "apple" },
      { streamName: "eclair", streamNamespace: "apple" },
    ]);

    expect(result.fieldsAdded).toEqual([
      {
        streamName: "harissa_paste",
        streamNamespace: "apple",
        fields: ["users.phone", "users.email"],
      },
    ]);

    expect(result.fieldsRemoved).toEqual([
      {
        streamName: "harissa_paste",
        streamNamespace: "apple",
        fields: ["users.lastName"],
      },
    ]);
  });
});
