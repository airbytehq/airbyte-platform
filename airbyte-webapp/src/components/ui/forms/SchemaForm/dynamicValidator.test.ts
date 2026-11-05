import { IntlShape } from "react-intl";
import { z } from "zod";

import { MAX_ARRAY_LENGTH } from "core/utils/zod";

import { convertJsonSchemaToZodSchema, dynamicValidator } from "./dynamicValidator";
import { AirbyteJsonSchema } from "./utils";

const formatMessage = (({ id }: { id: string }, values?: Record<string, unknown>) =>
  values ? `${id} ${JSON.stringify(values)}` : id) as IntlShape["formatMessage"];

const toZod = (schema: object, isRequired = true) =>
  convertJsonSchemaToZodSchema(
    schema as AirbyteJsonSchema,
    schema as AirbyteJsonSchema,
    formatMessage,
    isRequired,
    undefined
  );

// Only path and message reach the form, so the assertions ignore Zod's issue codes.
const issuesOf = (schema: z.ZodTypeAny, value: unknown) => {
  const result = schema.safeParse(value);
  if (result.success) {
    return [];
  }
  return result.error.issues.map(({ path, message }) => ({ path, message }));
};

const validate = (schema: object, data: Record<string, unknown>) =>
  dynamicValidator(schema as AirbyteJsonSchema, formatMessage)(data, undefined, {
    fields: {},
    shouldUseNativeValidation: false,
  });

describe("convertJsonSchemaToZodSchema", () => {
  describe("string", () => {
    it("rejects a missing or empty required string", () => {
      const schema = toZod({ type: "string" });
      expect(issuesOf(schema, undefined)).toEqual([{ path: [], message: "form.empty.error" }]);
      expect(issuesOf(schema, "")).toEqual([{ path: [], message: "form.empty.error" }]);
    });

    it("rejects a value that is not a string", () => {
      expect(issuesOf(toZod({ type: "string" }), 5)).toEqual([{ path: [], message: "form.invalid_type.error" }]);
    });

    it.each(["user@example.com", "first.last+tag@sub.example.co"])("accepts the email %s", (email) => {
      expect(issuesOf(toZod({ type: "string", format: "email" }), email)).toEqual([]);
    });

    it.each(["not-an-email", "user@", "user@example", "@example.com", "user@@example.com"])(
      "rejects the email %s",
      (email) => {
        expect(issuesOf(toZod({ type: "string", format: "email" }), email)).toEqual([
          { path: [], message: "form.email.error" },
        ]);
      }
    );

    it.each([
      "https://example.com",
      "http://localhost:8000/path?query=1",
      "https://user:pass@example.com:8443/a/b",
      "postgres://db.internal:5432/name",
      "s3://bucket/key",
    ])("accepts the URL %s", (url) => {
      expect(issuesOf(toZod({ type: "string", format: "uri" }), url)).toEqual([]);
    });

    it.each(["not a url", "example.com", "https//missing-colon.com"])("rejects the URL %s", (url) => {
      expect(issuesOf(toZod({ type: "string", format: "uri" }), url)).toEqual([
        { path: [], message: "form.url.error" },
      ]);
    });
  });

  describe("number and integer", () => {
    it("rejects a missing, NaN or null required number", () => {
      const schema = toZod({ type: "number" });
      expect(issuesOf(schema, undefined)).toEqual([{ path: [], message: "form.empty.error" }]);
      expect(issuesOf(schema, NaN)).toEqual([{ path: [], message: "form.empty.error" }]);
      expect(issuesOf(schema, null)).toEqual([{ path: [], message: "form.empty.error" }]);
    });

    it("rejects a value that is not a number", () => {
      expect(issuesOf(toZod({ type: "number" }), "12")).toEqual([{ path: [], message: "form.invalidNumber" }]);
    });

    it("accepts an integer above Number.MAX_SAFE_INTEGER", () => {
      expect(issuesOf(toZod({ type: "integer" }), 2 ** 53)).toEqual([]);
      expect(issuesOf(toZod({ type: "integer" }), 1.5)).toEqual([{ path: [], message: "form.invalidInteger" }]);
    });
  });

  describe("boolean", () => {
    it("rejects a missing required boolean and a non-boolean", () => {
      const schema = toZod({ type: "boolean" });
      expect(issuesOf(schema, undefined)).toEqual([{ path: [], message: "form.empty.error" }]);
      expect(issuesOf(schema, "true")).toEqual([{ path: [], message: "form.invalid_type.error" }]);
      expect(issuesOf(schema, false)).toEqual([]);
    });
  });

  describe("array", () => {
    it("rejects a missing required array and a non-array", () => {
      const schema = toZod({ type: "array", items: { type: "string" } });
      expect(issuesOf(schema, undefined)).toEqual([{ path: [], message: "form.empty.error" }]);
      expect(issuesOf(schema, "a")).toEqual([{ path: [], message: "form.invalid_type.error" }]);
    });

    it("applies minItems and maxItems together", () => {
      const schema = toZod({ type: "array", items: { type: "string" }, minItems: 2, maxItems: 3 });
      expect(issuesOf(schema, ["a", "b", "c"])).toEqual([]);
      expect(issuesOf(schema, ["a"])).toEqual([{ path: [], message: 'form.minItems.error {"min":2}' }]);
      expect(issuesOf(schema, ["a", "b", "c", "d"])).toEqual([{ path: [], message: 'form.maxItems.error {"max":3}' }]);
    });

    it.each([{ minItems: 2 }, { maxItems: 2 }, { minItems: 1, maxItems: 2 }])(
      "accepts a missing optional array that has %o",
      (bounds) => {
        expect(issuesOf(toZod({ type: "array", items: { type: "string" }, ...bounds }, false), undefined)).toEqual([]);
      }
    );

    it("rejects an array over maxItems with one issue, before it checks any item", () => {
      const schema = toZod({ type: "array", items: { type: "string" }, maxItems: 2 });
      expect(issuesOf(schema, [1, 2, 3])).toEqual([{ path: [], message: 'form.maxItems.error {"max":2}' }]);
    });

    it("limits an array without maxItems to MAX_ARRAY_LENGTH items", () => {
      const schema = toZod({ type: "array", items: { type: "string" } });
      expect(issuesOf(schema, Array(MAX_ARRAY_LENGTH).fill("a"))).toEqual([]);
      expect(issuesOf(schema, Array(MAX_ARRAY_LENGTH + 1).fill(1))).toEqual([
        { path: [], message: `form.maxItems.error {"max":${MAX_ARRAY_LENGTH}}` },
      ]);
    });

    it("caps a maxItems above MAX_ARRAY_LENGTH at MAX_ARRAY_LENGTH", () => {
      const schema = toZod({ type: "array", items: { type: "string" }, maxItems: MAX_ARRAY_LENGTH * 2 });
      expect(issuesOf(schema, Array(MAX_ARRAY_LENGTH + 1).fill(1))).toEqual([
        { path: [], message: `form.maxItems.error {"max":${MAX_ARRAY_LENGTH}}` },
      ]);
    });

    it("accepts only an empty array when maxItems is 0", () => {
      const schema = toZod({ type: "array", items: { type: "string" }, maxItems: 0 });
      expect(issuesOf(schema, [])).toEqual([]);
      expect(issuesOf(schema, ["a"])).toEqual([{ path: [], message: 'form.maxItems.error {"max":0}' }]);
    });
  });

  describe("object with properties", () => {
    const objectSchema = {
      type: "object",
      required: ["host", "port", "address"],
      properties: {
        host: { type: "string" },
        port: { type: "integer" },
        ssl: { type: "boolean" },
        anything: true,
        address: {
          type: "object",
          required: ["street"],
          properties: { street: { type: "string" }, city: { type: "string" } },
        },
      },
    };

    it("reports each missing, null or empty required property", () => {
      expect(issuesOf(toZod(objectSchema), { host: "", port: null })).toEqual([
        { path: ["host"], message: "form.empty.error" },
        { path: ["port"], message: "form.empty.error" },
        { path: ["address"], message: "form.empty.error" },
      ]);
    });

    it("accepts missing optional properties", () => {
      expect(issuesOf(toZod(objectSchema), { host: "h", port: 1, address: { street: "s" } })).toEqual([]);
    });

    it("reports a nested error at the nested path", () => {
      expect(issuesOf(toZod(objectSchema), { host: "h", port: 1, address: { city: "c" } })).toEqual([
        { path: ["address", "street"], message: "form.empty.error" },
      ]);
    });

    it("reports a property of the wrong type", () => {
      expect(issuesOf(toZod(objectSchema), { host: "h", port: "1", address: { street: "s" } })).toEqual([
        { path: ["port"], message: "form.invalidNumber" },
      ]);
    });
  });

  describe("object with an additionalProperties schema", () => {
    it("validates every value against that schema", () => {
      const schema = toZod({ type: "object", additionalProperties: { type: "string" } });
      expect(issuesOf(schema, { a: "x", b: "y" })).toEqual([]);
      expect(issuesOf(schema, { a: "x", b: 1 })).toEqual([{ path: ["b"], message: "form.invalid_type.error" }]);
    });
  });
});

describe("dynamicValidator", () => {
  it("returns the values when the data is valid", async () => {
    const schema = {
      type: "object",
      required: ["name", "port"],
      properties: {
        name: { type: "string", minLength: 2 },
        port: { type: "integer" },
        tags: { type: "array", items: { type: "string" }, maxItems: 1 },
      },
    };
    const result = await validate(schema, { name: "ab", port: 1 });
    expect(result.errors).toEqual({});
    expect(result.values).toEqual({ name: "ab", port: 1 });
  });
});
