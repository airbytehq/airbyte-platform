import { getDataType } from "./dataTypes";

describe("getDataType", () => {
  it.each([
    "string",
    "date",
    "timestamp_with_timezone",
    "timestamp_without_timezone",
    "datetime",
    "integer",
    "big_integer",
    "number",
    "big_number",
    "array",
    "object",
    "untyped",
    "union",
    "boolean",
  ])("should translate %s", (type) => {
    const current = getDataType({ type });
    expect(current).toMatchSnapshot();
    expect(current).not.toBe(type);
  });

  describe("union type", () => {
    it("should use union type if oneOf is set", () => {
      expect(getDataType({ type: "string", oneOf: [] })).toBe("airbyte.datatype.union");
    });
    it("should use union type if anyOf is set", () => {
      expect(getDataType({ type: "string", anyOf: [] })).toBe("airbyte.datatype.union");
    });
    it("should use union type if anyOf and oneOf is set", () => {
      expect(getDataType({ type: "string", anyOf: [], oneOf: [] })).toBe("airbyte.datatype.union");
    });
  });

  it("should return untyped when given no values", () => {
    // @ts-expect-error Using this method internally should demand a type
    expect(getDataType({})).toBe("airbyte.datatype.unknown");
  });

  describe("translation priority", () => {
    it("should use airbyte_type over format or type", () => {
      expect(getDataType({ type: "string", format: "date", airbyte_type: "big_number" })).toBe(
        "airbyte.datatype.big_number"
      );
    });
    it("should use format over type", () => {
      expect(getDataType({ type: "string", format: "date" })).toBe("airbyte.datatype.date");
    });
    it("should use oneOf/anyOf over everything else", () => {
      expect(getDataType({ type: "string", format: "date", airbyte_type: "big_number", oneOf: [] })).toBe(
        "airbyte.datatype.union"
      );
    });
  });
});
