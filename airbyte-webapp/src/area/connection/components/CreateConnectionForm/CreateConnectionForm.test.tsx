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

import { useDiscoverSchemaQuery } from "core/api";

import { CreateConnectionForm } from "./CreateConnectionForm";

const mockBaseUseDiscoverSchemaQuery = {
  error: null,
  isFetching: false,
  data: {
    catalog: mockConnection.syncCatalog,
    catalogId: "",
  },
  refetch: () => Promise.resolve(),
};

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "workspace-id",
  useCurrentWorkspaceLink: () => () => "/link/to/workspace",
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({}),
  useCurrentWorkspaceOrUndefined: () => ({
    workspaceId: "workspace-id",
    organizationId: "test-org-id",
  }),
  useInvalidateWorkspaceStateQuery: () => () => null,
  useCreateConnection: () => async () => null,
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  useDiscoverSourceSchemaMutation: jest.fn(() => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  })),
  useDiscoverSchemaQuery: jest.fn(() => mockBaseUseDiscoverSchemaQuery),
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
  useCurrentOrganizationInfo: () => ({
    organizationId: "test-org-id",
    organizationName: "Test Organization",
  }),
  useFirstOrg: () => ({
    organizationId: "test-org-id",
    organizationName: "Test Organization",
  }),
  useListPermissions: () => ({
    permissions: [],
    isLoading: false,
  }),
  useOrgInfo: () => ({
    billing: {
      paymentStatus: "okay",
    },
  }),
  useOrganizationTrialStatus: () => ({
    trialStatus: "in_trial",
    trialEndsAt: new Date(Date.now() + 86400000).toISOString(),
  }),
}));

jest.mock("area/connector/utils", () => ({
  useGetSourceFromSearchParams: () => mockConnection.source,
  useGetDestinationFromSearchParams: () => mockConnection.destination,
  ConnectorIds: jest.requireActual("area/connector/utils").ConnectorIds,
}));

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn().mockReturnValue({
    logout: jest.fn(),
    login: jest.fn(),
    isLoggedIn: jest.fn().mockReturnValue(true),
  }),
  useCurrentUser: jest.fn().mockReturnValue({
    userId: "test-user-id",
    email: "test@example.com",
  }),
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
    mocked(useDiscoverSchemaQuery).mockImplementationOnce(
      () =>
        ({
          ...mockBaseUseDiscoverSchemaQuery,
          isFetching: true,
        }) as unknown as ReturnType<typeof useDiscoverSchemaQuery>
    );
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  it("should render with an error", async () => {
    mocked(useDiscoverSchemaQuery).mockImplementationOnce(
      () =>
        ({
          ...mockBaseUseDiscoverSchemaQuery,
          error: new Error("Test Error"),
        }) as unknown as ReturnType<typeof useDiscoverSchemaQuery>
    );

    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });
});
