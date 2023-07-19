import { act, renderHook } from "@testing-library/react-hooks";
import React from "react";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import {
  mockDestinationDefinition,
  mockDestinationDefinitionSpecification,
} from "test-utils/mock-data/mockDestination";
import { mockSourceDefinition, mockSourceDefinitionSpecification } from "test-utils/mock-data/mockSource";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { TestWrapper } from "test-utils/testutils";

import { FormError } from "core/utils/errorStatusMessage";

import {
  ConnectionFormServiceProvider,
  ConnectionOrPartialConnection,
  useConnectionFormService,
} from "./ConnectionFormService";

jest.mock("services/connector/SourceDefinitionService", () => ({
  useSourceDefinition: () => mockSourceDefinition,
}));

jest.mock("services/connector/SourceDefinitionSpecificationService", () => ({
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
}));

jest.mock("services/connector/DestinationDefinitionSpecificationService", () => ({
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
}));

jest.mock("services/connector/DestinationDefinitionService", () => ({
  useDestinationDefinition: () => mockDestinationDefinition,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
}));

describe("ConnectionFormService", () => {
  const Wrapper: React.FC<Parameters<typeof ConnectionFormServiceProvider>[0]> = ({ children, ...props }) => (
    <TestWrapper>
      <ConnectionFormServiceProvider {...props}>{children}</ConnectionFormServiceProvider>
    </TestWrapper>
  );

  const refreshSchema = jest.fn();

  beforeEach(() => {
    refreshSchema.mockReset();
  });

  it("should take a partial Connection", async () => {
    const partialConnection: ConnectionOrPartialConnection = {
      syncCatalog: mockConnection.syncCatalog,
      source: mockConnection.source,
      destination: mockConnection.destination,
    };
    const { result } = renderHook(useConnectionFormService, {
      wrapper: Wrapper,
      initialProps: {
        connection: partialConnection,
        mode: "create",
        refreshSchema,
      },
    });

    expect(result.current.connection).toEqual(partialConnection);
  });

  it("should take a full Connection", async () => {
    const { result } = renderHook(useConnectionFormService, {
      wrapper: Wrapper,
      initialProps: {
        connection: mockConnection,
        mode: "create",
        refreshSchema,
      },
    });

    expect(result.current.connection).toEqual(mockConnection);
  });

  describe("Error Message Generation", () => {
    it("should return an error message if the form is invalid and dirty", async () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      expect(result.current.getErrorMessage(false)).toBe(
        "The form is invalid. Please make sure that all fields are correct."
      );
    });

    it("should not return an error message if the form is valid and dirty", async () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      expect(result.current.getErrorMessage(true)).toBe(null);
    });

    it("should return an error message if the form is invalid and not dirty", async () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      expect(result.current.getErrorMessage(false)).toBe(
        "The form is invalid. Please make sure that all fields are correct."
      );
    });

    it("should return an error message when given a submit error", () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      const errMsg = "asdf";
      act(() => {
        result.current.setSubmitError(new FormError(errMsg));
      });

      expect(result.current.getErrorMessage(false)).toBe(errMsg);
      expect(result.current.getErrorMessage(false)).toBe(errMsg);
      expect(result.current.getErrorMessage(true)).toBe(errMsg);
      expect(result.current.getErrorMessage(true)).toBe(errMsg);
    });

    it("should return an error message if the streams field is invalid", async () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      const errors = {
        syncCatalog: {
          streams: "connectionForm.streams.required",
        },
      };

      expect(result.current.getErrorMessage(false, errors)).toBe("Select at least 1 stream to sync.");
    });

    it("should not return an error message if the form is valid", async () => {
      const { result } = renderHook(useConnectionFormService, {
        wrapper: Wrapper,
        initialProps: {
          connection: mockConnection,
          mode: "create",
          refreshSchema,
        },
      });

      const errors = {
        syncCatalog: {
          streams: "There's an error",
        },
      };

      expect(result.current.getErrorMessage(true, errors)).toBe(null);
    });
  });
});
