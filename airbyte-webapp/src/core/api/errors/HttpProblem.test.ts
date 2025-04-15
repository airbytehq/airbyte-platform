import { renderHook } from "@testing-library/react";
import { useIntl } from "react-intl";

import { TestWrapper } from "test-utils";

import { HttpProblem } from "./HttpProblem";
import { KnownApiProblemTypeAndPrefixes } from "./problems";

jest.mock("locales/en.errors.json", () => ({
  validation: "Validation error: {reason}",
  "validation/invalid-email": "Invalid email: {reason}",
  "validation/invalid-email/already-exists": "Email already exists: {reason}",
  "http://airbyte.com/old-error": "Old error: {reason}",
}));

const request = {
  method: "GET" as const,
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

      expect(HttpProblem.isTypeOrSubtype(problem, "error:validation" as KnownApiProblemTypeAndPrefixes)).toBe(true);
    });

    it("should return true if type is a subtype", () => {
      const problem = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email",
        title: "bad-request",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any) as unknown;

      expect(HttpProblem.isTypeOrSubtype(problem, "error:validation" as KnownApiProblemTypeAndPrefixes)).toBe(true);
    });

    it("should return false if type does not match", () => {
      const problem = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email",
        title: "bad-request",
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any) as unknown;

      expect(HttpProblem.isTypeOrSubtype(problem, "error:permissions" as KnownApiProblemTypeAndPrefixes)).toBe(false);
    });
  });

  describe("error message", () => {
    const translate = (error: HttpProblem) => {
      return renderHook(
        () => {
          const { formatMessage } = useIntl();
          return error.translate(formatMessage);
        },
        { wrapper: TestWrapper }
      ).result.current;
    };

    /* eslint-disable @typescript-eslint/no-explicit-any */

    it("should use the exact match for legacy errors if available", () => {
      const error = new HttpProblem(request, 500, {
        type: "http://airbyte.com/old-error" as any,
        title: "legacy error" as any,
        data: {
          reason: "did not go well",
        },
      });
      expect(error).toHaveProperty("i18nType", "exact");
      expect(translate(error)).toBe("Old error: did not go well");
    });

    it("should not try hierarchy on legacy error types", () => {
      const error = new HttpProblem(request, 500, {
        type: "http://airbyte.com/old-error/sub-path" as any,
        title: "fallback msg" as any,
      });
      expect(error).toHaveProperty("i18nType", "title");
      expect(translate(error)).toBe("fallback msg");
    });

    it("should do exact matches on new error:type", () => {
      const error = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email/already-exists" as any,
        title: "fallback msg" as any,
        data: {
          reason: "see above",
        },
      });
      expect(error).toHaveProperty("i18nType", "exact");
      expect(translate(error)).toBe("Email already exists: see above");
    });

    it("should search through the hierarchy for error: types", () => {
      const error = new HttpProblem(request, 500, {
        type: "error:validation/invalid-email/at-is-missing" as any,
        title: "fallback msg" as any,
        data: {
          reason: "see above",
        },
      });
      expect(error).toHaveProperty("i18nType", "hierarchical");
      expect(translate(error)).toBe("Invalid email: see above");
    });

    it("should search multiple layers through the hierarchy for error: types", () => {
      const error = new HttpProblem(request, 500, {
        type: "error:validation/invalid-password/max-length" as any,
        title: "fallback msg" as any,
        data: {
          reason: "see above",
        },
      });
      expect(error).toHaveProperty("i18nType", "hierarchical");
      expect(translate(error)).toBe("Validation error: see above");
    });

    it("should use details if no hierarchical match can be found", () => {
      const error = new HttpProblem(request, 500, {
        type: "error:conflict/did-not-work" as any,
        title: "fallback msg" as any,
        detail: "fallback details" as any,
      });
      expect(error).toHaveProperty("i18nType", "detail");
      expect(translate(error)).toBe("fallback details");
    });

    it("should use title if no hierarchical match or detail can be found", () => {
      const error = new HttpProblem(request, 500, {
        type: "error:conflict/did-not-work" as any,
        title: "fallback msg" as any,
      });
      expect(error).toHaveProperty("i18nType", "title");
      expect(translate(error)).toBe("fallback msg");
    });

    /* eslint-enable @typescript-eslint/no-explicit-any */
  });
});
