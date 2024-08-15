import { resolveRefs } from "./utils";

describe("resolveRefs", () => {
  describe("should successfully resolve", () => {
    it("valid $ref", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
        },
        bar: {
          a: 1,
        },
      };
      const resolved = await resolveRefs(obj);
      expect(resolved).toEqual({
        foo: {
          a: 1,
        },
        bar: {
          a: 1,
        },
      });
    });

    it("nested $refs", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
        },
        bar: {
          nested: {
            $ref: "#/baz",
          },
        },
        baz: {
          a: 1,
        },
      };
      const resolved = await resolveRefs(obj);
      expect(resolved).toEqual({
        foo: {
          nested: {
            a: 1,
          },
        },
        bar: {
          nested: {
            a: 1,
          },
        },
        baz: {
          a: 1,
        },
      });
    });

    it("array $refs", async () => {
      const obj = {
        foo: [
          {
            $ref: "#/bar",
          },
          {
            $ref: "#/baz",
          },
        ],
        bar: {
          a: 1,
        },
        baz: {
          b: 2,
        },
      };
      const resolved = await resolveRefs(obj);
      expect(resolved).toEqual({
        foo: [
          {
            a: 1,
          },
          {
            b: 2,
          },
        ],
        bar: {
          a: 1,
        },
        baz: {
          b: 2,
        },
      });
    });

    it("non-circular $ref", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
          a: {
            b: 2,
            c: 3,
          },
        },
        bar: {
          $ref: "#/foo/a",
        },
      };
      const resolved = await resolveRefs(obj);
      expect(resolved).toEqual({
        foo: {
          b: 2,
          c: 3,
          a: {
            b: 2,
            c: 3,
          },
        },
        bar: {
          b: 2,
          c: 3,
        },
      });
    });

    it("with $ref sibling keys taking precedence over resolved $ref keys", async () => {
      const obj = {
        foo: {
          a: 1,
          b: 2,
          $ref: "#/bar",
        },
        bar: {
          a: 5,
          c: 3,
        },
      };
      const resolved = await resolveRefs(obj);
      expect(resolved).toEqual({
        foo: {
          a: 1,
          b: 2,
          c: 3,
        },
        bar: {
          a: 5,
          c: 3,
        },
      });
    });
  });

  describe("should throw error for", () => {
    it("non-string ref", async () => {
      const obj = {
        foo: {
          $ref: {
            a: 1,
          },
        },
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });

    it("unfound ref", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
        },
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });

    it("invalid ref syntax", async () => {
      const obj = {
        foo: {
          $ref: "/bar",
        },
        bar: {
          a: 1,
        },
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });

    it("refs that do not point to an object", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
        },
        bar: "not an object",
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });

    it("direct circular refs", async () => {
      const obj = {
        foo: {
          $ref: "#/bar",
        },
        bar: {
          $ref: "#/foo",
        },
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });

    it("indirect circular refs", async () => {
      const obj = {
        foo: {
          a: {
            $ref: "#/bar",
          },
        },
        bar: {
          b: {
            c: {
              $ref: "#/baz",
            },
          },
        },
        baz: {
          d: {
            $ref: "#/foo",
          },
        },
      };
      await expect(() => resolveRefs(obj)).rejects.toThrow();
    });
  });
});
