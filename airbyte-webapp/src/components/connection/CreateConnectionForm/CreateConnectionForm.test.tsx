import { act, render as tlr } from "@testing-library/react";
import React from "react";
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
import { mocked, TestWrapper, useMockIntersectionObserver } from "test-utils/testutils";

import { useDiscoverSchema } from "core/api";

import { CreateConnectionForm } from "./CreateConnectionForm";

const mockBaseUseDiscoverSchema = {
  schemaErrorStatus: null,
  isLoading: false,
  schema: mockConnection.syncCatalog,
  catalogId: "",
  onDiscoverSchema: () => Promise.resolve(),
};

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "workspace-id",
  useCurrentWorkspaceLink: () => () => "/link/to/workspace",
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({}),
  useInvalidateWorkspaceStateQuery: () => () => null,
  useCreateConnection: () => async () => null,
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  useDiscoverSchema: jest.fn(() => mockBaseUseDiscoverSchema),
  ErrorWithJobInfo: jest.requireActual("core/api/errors").ErrorWithJobInfo,
  useDescribeCronExpressionFetchQuery: () => async () => ({
    isValid: true,
    cronDescription: "every hour",
    nextExecutions: [],
  }),
  useGetDataplaneGroup: () => mockGetDataplaneGroup,
  useGetWebappConfig: () => ({
    version: "test-version",
    edition: "community",
  }),
}));

jest.mock("area/connector/utils", () => ({
  useGetSourceFromSearchParams: () => mockConnection.source,
  useGetDestinationFromSearchParams: () => mockConnection.destination,
  ConnectorIds: jest.requireActual("area/connector/utils").ConnectorIds,
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.setTimeout(40000);

describe("CreateConnectionForm", () => {
  const Wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <TestWrapper>
      <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
        {children}
      </VirtuosoMockContext.Provider>
    </TestWrapper>
  );
  const render = async () => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper>
          <CreateConnectionForm />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  beforeEach(() => {
    useMockIntersectionObserver();
  });

  it("should render", async () => {
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
    expect(renderResult.queryByText("Please wait a little bit more…")).toBeFalsy();
  });

  it("should render when loading", async () => {
    mocked(useDiscoverSchema).mockImplementationOnce(() => ({ ...mockBaseUseDiscoverSchema, isLoading: true }));
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  it("should render with an error", async () => {
    mocked(useDiscoverSchema).mockImplementationOnce(() => ({
      ...mockBaseUseDiscoverSchema,
      schemaErrorStatus: new Error("Test Error"),
    }));

    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });
});
