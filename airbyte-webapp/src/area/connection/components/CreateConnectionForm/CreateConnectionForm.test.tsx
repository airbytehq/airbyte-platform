import { act, fireEvent, render as tlr, screen, waitFor } from "@testing-library/react";
import cloneDeep from "lodash/cloneDeep";
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

import { ConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn";
import { useConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess";
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

const mockUseGetSourceFromSearchParams = jest.fn(() => mockConnection.source);
const mockUseGetDestinationFromSearchParams = jest.fn(() => mockConnection.destination);
const mockUseOrganizationPlan = jest.fn();
const mockCreateConnection = jest.fn();
const mockCreateSemanticSearchEnablement = jest.fn();
const mockNavigate = jest.fn();

const defaultOrganizationPlanFlags = {
  isStiggPlanEnabled: false,
  isStandardTrialPlan: false,
  isStandardPlan: false,
  isPlusPlan: false,
  isFlexPlan: false,
  isProPlan: false,
};

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "workspace-id",
  useCurrentWorkspaceLink: () => () => "/link/to/workspace",
}));

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils"),
  useOrganizationPlan: () => mockUseOrganizationPlan(),
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({}),
  useCurrentWorkspaceOrUndefined: () => ({
    workspaceId: "workspace-id",
    organizationId: "test-org-id",
  }),
  useInvalidateWorkspaceStateQuery: () => () => null,
  useCreateConnection: () => ({ mutateAsync: mockCreateConnection }),
  useCreateConnectionSemanticSearchEnablement: () => ({ mutateAsync: mockCreateSemanticSearchEnablement }),
  // A submit with a basic schedule refreshes the new connection's status 2 seconds later. That timer can fire during a
  // later test, so the key it reads must exist.
  connectionsKeys: { statuses: (connectionIds: string[]) => ["connections", "statuses", connectionIds] },
  useTagsList: () => [],
  useCreateTag: () => ({ mutateAsync: jest.fn() }),
  useDescribeCronExpression: () => ({ data: undefined }),
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
  useGetSourceFromSearchParams: () => mockUseGetSourceFromSearchParams(),
  useGetDestinationFromSearchParams: () => mockUseGetDestinationFromSearchParams(),
  ConnectorIds: jest.requireActual("area/connector/utils").ConnectorIds,
}));

jest.mock("cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess", () => ({
  useConnectionSemanticSearchAccess: jest.fn(),
}));

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockNavigate,
}));

const mockUseAccess = useConnectionSemanticSearchAccess as jest.MockedFunction<
  typeof useConnectionSemanticSearchAccess
>;

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
  const Wrapper: React.FC<React.PropsWithChildren<{ route?: string }>> = ({ children, route }) => (
    <TestWrapper route={route}>
      <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
        {children}
      </VirtuosoMockContext.Provider>
    </TestWrapper>
  );
  const render = async (route?: string) => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper route={route}>
          <CreateConnectionForm />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  beforeEach(() => {
    useMockIntersectionObserver();
    mockUseGetSourceFromSearchParams.mockReturnValue(mockConnection.source);
    mockUseGetDestinationFromSearchParams.mockReturnValue(mockConnection.destination);
    mockUseOrganizationPlan.mockReturnValue(defaultOrganizationPlanFlags);
    mockUseAccess.mockReturnValue("hidden");
    mockCreateConnection.mockReset().mockResolvedValue({ connectionId: "new-connection-id" });
    mockCreateSemanticSearchEnablement.mockReset().mockResolvedValue({ enable_indexing: true });
    mockNavigate.mockReset();
  });

  it("should render", async () => {
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
    expect(renderResult.queryByText("Please wait a little bit more…")).toBeFalsy();
  });

  it.each([
    ["Standard", "isStandardPlan"],
    ["Standard Trial", "isStandardTrialPlan"],
    ["Plus", "isPlusPlan"],
  ] as const)("shows the initial sync volume warning for %s", async (_plan, planFlag) => {
    mockUseOrganizationPlan.mockReturnValue({
      ...defaultOrganizationPlanFlags,
      [planFlag]: true,
    });

    const renderResult = await render();

    expect(renderResult.getByText("Initial sync will replicate all data in enabled streams")).toBeInTheDocument();
  });

  it.each([
    [
      "Pro",
      {
        ...defaultOrganizationPlanFlags,
        isStiggPlanEnabled: true,
        isProPlan: true,
      },
    ],
    [
      "Flex",
      {
        ...defaultOrganizationPlanFlags,
        isStiggPlanEnabled: true,
        isFlexPlan: true,
      },
    ],
    ["OSS/SME without a plan", defaultOrganizationPlanFlags],
  ])("does not show the initial sync volume warning for %s", async (_plan, planFlags) => {
    mockUseOrganizationPlan.mockReturnValue(planFlags);

    const renderResult = await render();

    expect(renderResult.queryByText("Initial sync will replicate all data in enabled streams")).not.toBeInTheDocument();
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

  it("blocks schema discovery when the destination is still a draft", async () => {
    mockUseGetDestinationFromSearchParams.mockReturnValue({ ...mockConnection.destination, isDraft: true });

    const renderResult = await render();

    expect(useDiscoverSchemaQuery).toHaveBeenCalledWith(mockConnection.source, {
      enabled: false,
      useErrorBoundary: false,
    });
    expect(
      renderResult.getByText(
        "The source or destination is still in draft and isn't usable in a connection yet. Finish setting up the connector first, then create a connection."
      )
    ).toBeVisible();
  });

  describe("semantic search", () => {
    // The "Configure connection" step, where the semantic search field and the submit button are.
    const CONFIGURE_STEP_ROUTE = "/continued";

    // One selected stream with a primary key, so the form is valid and can be submitted.
    const [firstStream] = mockConnection.syncCatalog.streams;
    const validStream = {
      stream: { ...firstStream.stream!, sourceDefinedPrimaryKey: [["id"]] },
      config: { ...firstStream.config!, primaryKey: [["id"]], selected: true },
    };

    beforeEach(() => {
      mocked(useDiscoverSchemaQuery).mockImplementation(
        () =>
          ({
            ...mockBaseUseDiscoverSchemaQuery,
            data: { catalog: { streams: [cloneDeep(validStream)] }, catalogId: "" },
          }) as unknown as ReturnType<typeof useDiscoverSchemaQuery>
      );
    });

    afterEach(() => {
      mocked(useDiscoverSchemaQuery).mockImplementation(
        () => mockBaseUseDiscoverSchemaQuery as unknown as ReturnType<typeof useDiscoverSchemaQuery>
      );
    });

    const submit = async (access: ConnectionSemanticSearchAccess) => {
      mockUseAccess.mockReturnValue(access);
      await render(CONFIGURE_STEP_ROUTE);
      const submitButton = await screen.findByRole("button", { name: "Finish & Sync" });
      await waitFor(() => expect(submitButton).toBeEnabled());
      await act(async () => {
        fireEvent.click(submitButton);
      });
      await waitFor(() => expect(mockNavigate).toHaveBeenCalledWith("../../connections/new-connection-id"));
    };

    it("turns semantic search on after create when the viewer can edit", async () => {
      await submit("editable");

      expect(mockUseAccess).toHaveBeenCalledWith({
        sourceDefinitionId: mockConnection.source.sourceDefinitionId,
        destinationDefinitionId: mockConnection.destination.destinationDefinitionId,
      });
      expect(mockCreateConnection).toHaveBeenCalledWith(
        expect.objectContaining({ values: expect.objectContaining({ semanticSearch: true }) })
      );
      expect(mockCreateSemanticSearchEnablement).toHaveBeenCalledWith({
        connectionId: "new-connection-id",
        enabled: true,
      });
    });

    it("does not save semantic search when the user turned it off", async () => {
      mockUseAccess.mockReturnValue("editable");
      await render(CONFIGURE_STEP_ROUTE);
      fireEvent.click(await screen.findByRole("checkbox", { name: /Semantic search/ }));
      const submitButton = screen.getByRole("button", { name: "Finish & Sync" });
      await act(async () => {
        fireEvent.click(submitButton);
      });

      await waitFor(() => expect(mockNavigate).toHaveBeenCalledWith("../../connections/new-connection-id"));
      expect(mockCreateSemanticSearchEnablement).not.toHaveBeenCalled();
    });

    it.each(["hidden", "orgOff", "noPermission"] as const)(
      "does not save semantic search when the access is %s",
      async (access) => {
        await submit(access);

        expect(mockCreateConnection).toHaveBeenCalled();
        expect(mockCreateSemanticSearchEnablement).not.toHaveBeenCalled();
      }
    );

    it("shows an error and still opens the new connection when semantic search cannot be saved", async () => {
      mockCreateSemanticSearchEnablement.mockRejectedValue(new Error("enablement failed"));

      await submit("editable");

      expect(mockCreateSemanticSearchEnablement).toHaveBeenCalled();
      expect(
        await screen.findByText(
          "Connection created, but semantic search could not be turned on. You can turn it on in the connection settings."
        )
      ).toBeInTheDocument();
    });
  });
});
