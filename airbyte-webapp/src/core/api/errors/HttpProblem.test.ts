import { HttpProblem } from "./HttpProblem";

const request = {
  method: "get" as const,
  url: "/api",
};

const noop = (_: unknown): void => {};

describe("HttpProblem", () => {
  describe("#isInstanceOf", () => {
    it("should return true for HttpProblem", () => {
      const problem = new HttpProblem(request, 500, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
      }) as unknown;

      expect(HttpProblem.isInstanceOf(problem)).toBe(true);
    });

    // eslint-disable-next-line jest/expect-expect
    it("should deliver better typing than the instanceof operator", () => {
      const problem = new HttpProblem(request, 500, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
      }) as unknown;

      // Additional TypeScript verifications (they validate at compile time and are not really runtime tests):
      if (problem instanceof HttpProblem) {
        // With instanceof it turns the generic type into `any` and thus anything would be accesible without a type error.
        noop(problem.response.nonExistingProperty);
        // Making sure to optional call those since they don't really exist.
        noop(problem.response.type?.defDoesNotExist?.());
      }

      if (HttpProblem.isInstanceOf(problem)) {
        // @ts-expect-error This must cause a TS compile time error otherwise our isInstanceOf method didn't work properly anymore.
        noop(problem.response.nonExistingProperty);
        // This should work since `.type` should be properly typed as a string.
        problem.response.type.startsWith("");
        // @ts-expect-error This should not exist, since it knows `type` should be a string union
        problem.response.type.defDoesNotExist?.();
      }
    });
  });

  describe("#isType", () => {
    it("should return true if type matches", () => {
      const problem = new HttpProblem(request, 500, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
      }) as unknown;

      expect(HttpProblem.isType(problem, "https://reference.airbyte.com/reference/errors#bad-request")).toBe(true);
    });

    it("should return false if type does not match", () => {
      const problem = new HttpProblem(request, 500, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
      }) as unknown;

      expect(HttpProblem.isType(problem, "https://reference.airbyte.com/reference/errors#forbidden")).toBe(false);
    });

    // eslint-disable-next-line jest/expect-expect
    it("should narrow down TypeScript type properly", () => {
      const problem = new HttpProblem(request, 500, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
      }) as unknown;

      if (HttpProblem.isType(problem, "https://reference.airbyte.com/reference/errors#bad-request")) {
        noop(problem.response.title === "bad-request");
        // @ts-expect-error This must cause a TS compile time error otherwise isType didn't narrow the type well enough
        noop(problem.response.type === "https://reference.airbyte.com/reference/errors#forbidden");
        // @ts-expect-error This must cause a TS compile time error otherwise isType didn't narrow the type well enough
        noop(problem.response.title === "forbidden");
      }
    });
  });

  describe("#isTypeOrSubtype", () => {
    it("should return true if type matches", () => {
      const problem = new HttpProblem(request, 500, {
        type: "error:validation",
        title: "bad-request",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any) as unknown;

      expect(HttpProblem.isTypeOrSubtype(problem, "error:validation")).toBe(true);
    });

    it("should return true if type is a subtype", () => {
      const problem = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email",
        title: "bad-request",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any) as unknown;

      expect(HttpProblem.isTypeOrSubtype(problem, "error:validation")).toBe(true);
    });

    it("should return false if type does not match", () => {
      const problem = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email",
        title: "bad-request",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any) as unknown;

      expect(HttpProblem.isTypeOrSubtype(problem, "error:permissions")).toBe(false);
    });
  });
});
