import { render as tlr, act } from "@testing-library/react";
import React, { Suspense } from "react";
import { VirtuosoMockContext } from "react-virtuoso";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockGetDataplaneGroup } from "test-utils/mock-data/mockDataplaneGroups";
import {
  mockDestinationDefinition,
  mockDestinationDefinitionSpecification,
  mockDestinationDefinitionVersion,
} from "test-utils/mock-data/mockDestination";
import {
  mockSourceDefinition,
  mockSourceDefinitionSpecification,
  mockSourceDefinitionVersion,
} from "test-utils/mock-data/mockSource";
import { mockTheme } from "test-utils/mock-data/mockTheme";
import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { mockWorkspaceId } from "test-utils/mock-data/mockWorkspaceId";
import { TestWrapper, useMockIntersectionObserver } from "test-utils/testutils";

import { useGetConnectionQuery } from "core/api";
import { WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { ConnectionEditServiceProvider } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { ConnectionReplicationPage } from "./ConnectionReplicationPage";

jest.setTimeout(40000);

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => mockWorkspaceId,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetConnectionQuery: jest.fn(() => async () => mockConnection),
  useGetConnection: () => mockConnection,
  useCurrentConnection: () => mockConnection,
  useGetStateTypeQuery: () => async () => "global",
  useUpdateConnection: () => ({
    mutateAsync: async (connection: WebBackendConnectionUpdate) => connection,
    isLoading: false,
  }),
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  ErrorWithJobInfo: jest.requireActual("core/api/errors").ErrorWithJobInfo,
  useDescribeCronExpressionFetchQuery: () => async () => ({
    isValid: true,
    cronDescription: "every hour",
    nextExecutions: [],
  }),
  useGetDataplaneGroup: () => mockGetDataplaneGroup,
  useGetWebappConfig: () => mockWebappConfig,
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: () => true,
  useGeneratedIntent: () => true,
  Intent: {
    CreateOrEditConnection: "CreateOrEditConnection",
  },
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

describe("ConnectionReplicationPage", () => {
  const Wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <Suspense fallback={<div>I should not show up in a snapshot</div>}>
      <TestWrapper>
        <ConnectionEditServiceProvider connectionId={mockConnection.connectionId}>
          <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
            {children}
          </VirtuosoMockContext.Provider>
        </ConnectionEditServiceProvider>
      </TestWrapper>
    </Suspense>
  );

  const render = async () => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper>
          <ConnectionReplicationPage />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  const setupSpies = (getConnection?: () => Promise<void>) => {
    (useGetConnectionQuery as jest.Mock).mockImplementation(() => getConnection);
  };

  beforeEach(() => {
    useMockIntersectionObserver();
  });

  it("should render", async () => {
    setupSpies();

    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  it("should show an error if there is a schemaError", async () => {
    setupSpies(() => Promise.reject("Test Error"));

    const renderResult = await render();

    await act(async () => {
      renderResult.queryByText("Refresh source schema")?.click();
    });
    expect(renderResult).toMatchSnapshot();
  });

  it("should show loading if the schema is refreshing", async () => {
    // Return pending promise
    setupSpies(() => new Promise(() => null));

    const renderResult = await render();
    await act(async () => {
      renderResult.queryByText("Refresh source schema")?.click();
    });

    await act(async () => {
      expect(renderResult.findByText("We are fetching the schema of your data source.", { exact: false })).toBeTruthy();
    });
  });
});
