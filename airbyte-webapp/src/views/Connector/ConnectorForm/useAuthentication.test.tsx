import { renderHook } from "@testing-library/react";
import get from "lodash/get";
import { FieldError, useFormContext } from "react-hook-form";

import { SourceDefinitionSpecificationRead } from "core/api/types/AirbyteClient";

import { useConnectorForm } from "./connectorFormContext";
import { useAuthentication } from "./useAuthentication";
import { noPredicateAdvancedAuth, predicateInsideConditional } from "./useAuthentication.mocks";
import { makeConnectionConfigurationPath } from "./utils";

jest.mock("./connectorFormContext");
jest.mock("react-hook-form", () => ({
  ...jest.requireActual("react-hook-form"),
  useFormContext: jest.fn(),
}));

const mockConnectorForm = useConnectorForm as unknown as jest.Mock<Partial<ReturnType<typeof useConnectorForm>>>;
const mockFormContext = useFormContext as unknown as jest.Mock<Partial<ReturnType<typeof useFormContext>>>;

interface MockParams {
  connector: Pick<SourceDefinitionSpecificationRead, "advancedAuth" | "connectionSpecification">;
  values: unknown;
  submitCount?: number;
  fieldMeta?: Record<string, { error?: FieldError }>;
}

const mockContext = ({ connector, values, submitCount, fieldMeta = {} }: MockParams) => {
  mockFormContext.mockReturnValue({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    watch: ((field: string) => (field ? get(values, field) : values)) as any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    formState: { submitCount: submitCount ?? 0 } as any,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    getFieldState: (field) => (fieldMeta[field] ?? {}) as any,
  });
  mockConnectorForm.mockReturnValue({
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    selectedConnectorDefinitionSpecification: { ...connector, sourceDefinitionId: "12345", jobInfo: {} as any },
    getValues: (values) => values,
  });
};

describe("useAuthentication", () => {
  it("should return empty results for non OAuth connectors", () => {
    mockContext({ connector: {}, values: {} });
    const { result } = renderHook(() => useAuthentication());
    expect(result.current.hiddenAuthFieldErrors).toEqual({});
    expect(result.current.shouldShowAuthButton("field")).toBe(false);
    expect(result.current.isHiddenAuthField("field")).toBe(false);
  });

  describe("for advancedAuth connectors", () => {
    describe("without a predicateKey", () => {
      it("should calculate hiddenAuthFields correctly", () => {
        mockContext({ connector: { advancedAuth: noPredicateAdvancedAuth }, values: {} });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["access_token"]))).toBe(true);
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["client_id"]))).toBe(false);
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["client_secret"]))).toBe(false);
      });

      it("should show the auth button on the root level", () => {
        mockContext({ connector: { advancedAuth: noPredicateAdvancedAuth }, values: {} });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.shouldShowAuthButton(makeConnectionConfigurationPath())).toBe(true);
      });

      it("should not return authErrors before submitting", () => {
        const accessTokenField = makeConnectionConfigurationPath(["access_token"]);
        mockContext({
          connector: { advancedAuth: noPredicateAdvancedAuth },
          values: {},
          fieldMeta: { [accessTokenField]: { error: { type: "required", message: "Field is required" } } },
          submitCount: 0,
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.hiddenAuthFieldErrors).toEqual({});
      });

      it("should return existing authErrors if submitted once", () => {
        const accessTokenField = makeConnectionConfigurationPath(["access_token"]);
        mockContext({
          connector: { advancedAuth: noPredicateAdvancedAuth },
          values: {},
          fieldMeta: { [accessTokenField]: { error: { type: "required", message: "Field is empty" } } },
          submitCount: 1,
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.hiddenAuthFieldErrors).toEqual({ [accessTokenField]: "required" });
      });
    });

    describe("with predicateKey inside conditional", () => {
      it("should hide auth fields when predicate value matches", () => {
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "oauth2.0" } } },
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "access_token"]))).toBe(
          true
        );
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "client_id"]))).toBe(
          true
        );
        expect(
          result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "client_secret"]))
        ).toBe(true);
      });

      it("should not hide auth fields when predicate value is a mismatch", () => {
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "token" } } },
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "access_token"]))).toBe(
          false
        );
        expect(result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "client_id"]))).toBe(
          false
        );
        expect(
          result.current.isHiddenAuthField(makeConnectionConfigurationPath(["credentials", "client_secret"]))
        ).toBe(false);
      });

      it("should show the auth button inside the conditional if right option is selected", () => {
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "oauth2.0" } } },
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.shouldShowAuthButton(makeConnectionConfigurationPath(["credentials", "auth_type"]))).toBe(
          true
        );
      });

      it("shouldn't show the auth button if the wrong conditional option is selected", () => {
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "token" } } },
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.shouldShowAuthButton(makeConnectionConfigurationPath(["credentials", "auth_type"]))).toBe(
          false
        );
      });

      it("should not return authErrors before submitting", () => {
        const accessTokenField = makeConnectionConfigurationPath(["credentials", "access_token"]);
        const clientIdField = makeConnectionConfigurationPath(["credentials", "client_id"]);
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "oauth2.0" } } },
          fieldMeta: {
            [accessTokenField]: { error: { type: "required", message: "Field is empty" } },
            [clientIdField]: { error: { type: "validate", message: "Another validation error" } },
          },
          submitCount: 0,
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.hiddenAuthFieldErrors).toEqual({});
      });

      it("should return authErrors when conditional has correct option selected", () => {
        const accessTokenField = makeConnectionConfigurationPath(["credentials", "access_token"]);
        const clientIdField = makeConnectionConfigurationPath(["credentials", "client_id"]);
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "oauth2.0" } } },
          fieldMeta: {
            [accessTokenField]: { error: { type: "required", message: "Field is empty" } },
            [clientIdField]: { error: { type: "validate", message: "Another validation error" } },
          },
          submitCount: 1,
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.hiddenAuthFieldErrors).toEqual({
          [accessTokenField]: "required",
          [clientIdField]: "validate",
        });
      });

      it("should not return authErrors when conditional has the incorrect option selected", () => {
        const accessTokenField = makeConnectionConfigurationPath(["credentials", "access_token"]);
        const clientIdField = makeConnectionConfigurationPath(["credentials", "client_id"]);
        mockContext({
          connector: { advancedAuth: predicateInsideConditional },
          values: { connectionConfiguration: { credentials: { auth_type: "token" } } },
          fieldMeta: {
            [accessTokenField]: { error: { type: "required", message: "Field is empty" } },
            [clientIdField]: { error: { type: "validate", message: "Another validation error" } },
          },
          submitCount: 1,
        });
        const { result } = renderHook(() => useAuthentication());
        expect(result.current.hiddenAuthFieldErrors).toEqual({});
      });
    });
  });
});
