import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, screen, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { TestSuspenseBoundary } from "test-utils/TestSuspenseBoundary";

import { useCancelUserInvitation, useListUserInvitations } from "./userInvitations";
import { cancelUserInvitation, listPendingInvitations } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION, SCOPE_WORKSPACE } from "../scopes";
import {
  PermissionType,
  ScopeType,
  UserInvitationAdminRead,
  UserInvitationListRequestBody,
  UserInvitationStatus,
} from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  listPendingInvitations: jest.fn(),
  cancelUserInvitation: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("core/services/Notification", () => ({
  useNotificationService: jest.fn(() => ({ registerNotification: jest.fn() })),
}));

jest.mock("react-intl", () => ({
  ...jest.requireActual("react-intl"),
  useIntl: jest.fn(() => ({ formatMessage: ({ id }: { id: string }) => id })),
}));

const mockListPendingInvitations = listPendingInvitations as jest.MockedFunction<typeof listPendingInvitations>;
const mockCancelUserInvitation = cancelUserInvitation as jest.MockedFunction<typeof cancelUserInvitation>;

const buildInvitation = (id: string, scopeType: ScopeType, scopeId: string): UserInvitationAdminRead => ({
  id,
  inviterUserId: "inviter-id",
  invitedEmail: `${id}@example.com`,
  scopeId,
  scopeType,
  permissionType: PermissionType.organization_member,
  status: UserInvitationStatus.pending,
  createdAt: 0,
  updatedAt: 0,
});

const ORGANIZATION_A: UserInvitationListRequestBody = { scopeType: ScopeType.organization, scopeId: "organization-a" };
const ORGANIZATION_B: UserInvitationListRequestBody = { scopeType: ScopeType.organization, scopeId: "organization-b" };
const WORKSPACE_A: UserInvitationListRequestBody = { scopeType: ScopeType.workspace, scopeId: "workspace-a" };
const WORKSPACE_B: UserInvitationListRequestBody = { scopeType: ScopeType.workspace, scopeId: "workspace-b" };

const INVITATIONS_A = [buildInvitation("invitation-a", ScopeType.organization, ORGANIZATION_A.scopeId)];
const INVITATIONS_B = [buildInvitation("invitation-b", ScopeType.organization, ORGANIZATION_B.scopeId)];

const createWrapper = (queryClient: QueryClient) => {
  const Wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <TestSuspenseBoundary>{children}</TestSuspenseBoundary>
    </QueryClientProvider>
  );
  return Wrapper;
};

const mockInvitationsByScopeId = (
  invitationsByScopeId: Record<string, UserInvitationAdminRead[] | Promise<UserInvitationAdminRead[]>>
) => {
  mockListPendingInvitations.mockImplementation(({ scopeId }) =>
    Promise.resolve(invitationsByScopeId[scopeId]).then((invitations) => invitations)
  );
};

describe("useListUserInvitations", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  it("should return the invitations of the requested organization", async () => {
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A });

    const { result } = renderHook(() => useListUserInvitations(ORGANIZATION_A), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_A));
  });

  it("should show the loading state while the invitations of the newly selected organization load", async () => {
    const pendingInvitationsB = new Promise<UserInvitationAdminRead[]>(() => undefined);

    mockInvitationsByScopeId({
      [ORGANIZATION_A.scopeId]: INVITATIONS_A,
      [ORGANIZATION_B.scopeId]: pendingInvitationsB,
    });

    const { result, rerender } = renderHook(
      ({ request }: { request: UserInvitationListRequestBody }) => useListUserInvitations(request),
      { wrapper: createWrapper(queryClient), initialProps: { request: ORGANIZATION_A } }
    );

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_A));

    rerender({ request: ORGANIZATION_B });

    expect(await screen.findByText("Test suspense boundary")).toBeInTheDocument();
  });

  it("should request the invitations of the newly selected organization", async () => {
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A, [ORGANIZATION_B.scopeId]: INVITATIONS_B });

    const { result, rerender } = renderHook(
      ({ request }: { request: UserInvitationListRequestBody }) => useListUserInvitations(request),
      { wrapper: createWrapper(queryClient), initialProps: { request: ORGANIZATION_A } }
    );

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_A));

    rerender({ request: ORGANIZATION_B });

    await waitFor(() => expect(mockListPendingInvitations).toHaveBeenLastCalledWith(ORGANIZATION_B, {}));
  });

  it("should return the invitations of the newly selected organization once they are loaded", async () => {
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A, [ORGANIZATION_B.scopeId]: INVITATIONS_B });

    const { result, rerender } = renderHook(
      ({ request }: { request: UserInvitationListRequestBody }) => useListUserInvitations(request),
      { wrapper: createWrapper(queryClient), initialProps: { request: ORGANIZATION_A } }
    );

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_A));

    rerender({ request: ORGANIZATION_B });

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_B));
  });

  it("should cache the invitations of each organization separately", async () => {
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A, [ORGANIZATION_B.scopeId]: INVITATIONS_B });

    const { result, rerender } = renderHook(
      ({ request }: { request: UserInvitationListRequestBody }) => useListUserInvitations(request),
      { wrapper: createWrapper(queryClient), initialProps: { request: ORGANIZATION_A } }
    );

    await waitFor(() => expect(result.current).toEqual(INVITATIONS_A));
    rerender({ request: ORGANIZATION_B });
    await waitFor(() => expect(result.current).toEqual(INVITATIONS_B));

    expect([
      queryClient.getQueryData([SCOPE_ORGANIZATION, "userInvitations", ORGANIZATION_A.scopeId]),
      queryClient.getQueryData([SCOPE_ORGANIZATION, "userInvitations", ORGANIZATION_B.scopeId]),
    ]).toEqual([INVITATIONS_A, INVITATIONS_B]);
  });

  it("should show the loading state while the invitations of the newly selected workspace load", async () => {
    const pendingInvitationsB = new Promise<UserInvitationAdminRead[]>(() => undefined);
    const workspaceInvitationsA = [buildInvitation("invitation-workspace-a", ScopeType.workspace, WORKSPACE_A.scopeId)];
    mockInvitationsByScopeId({
      [WORKSPACE_A.scopeId]: workspaceInvitationsA,
      [WORKSPACE_B.scopeId]: pendingInvitationsB,
    });
    const { result, rerender } = renderHook(
      ({ request }: { request: UserInvitationListRequestBody }) => useListUserInvitations(request),
      { wrapper: createWrapper(queryClient), initialProps: { request: WORKSPACE_A } }
    );
    await waitFor(() => expect(result.current).toEqual(workspaceInvitationsA));

    rerender({ request: WORKSPACE_B });

    expect(await screen.findByText("Test suspense boundary")).toBeInTheDocument();
  });

  it("should not share cached invitations between an organization and a workspace", async () => {
    const workspaceInvitations = [buildInvitation("invitation-workspace", ScopeType.workspace, WORKSPACE_A.scopeId)];
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A, [WORKSPACE_A.scopeId]: workspaceInvitations });
    const { result: organizationResult } = renderHook(() => useListUserInvitations(ORGANIZATION_A), {
      wrapper: createWrapper(queryClient),
    });
    await waitFor(() => expect(organizationResult.current).toEqual(INVITATIONS_A));

    const { result: workspaceResult } = renderHook(() => useListUserInvitations(WORKSPACE_A), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(workspaceResult.current).toEqual(workspaceInvitations));
  });

  it("should refetch the invitations of the current organization after cancelling an invitation", async () => {
    mockInvitationsByScopeId({ [ORGANIZATION_A.scopeId]: INVITATIONS_A });
    mockCancelUserInvitation.mockResolvedValue(INVITATIONS_A[0]);
    const { result } = renderHook(
      () => ({ invitations: useListUserInvitations(ORGANIZATION_A), cancel: useCancelUserInvitation() }),
      { wrapper: createWrapper(queryClient) }
    );
    await waitFor(() => expect(result.current.invitations).toEqual(INVITATIONS_A));

    await result.current.cancel.mutateAsync({ id: INVITATIONS_A[0].id });

    await waitFor(() => expect(mockListPendingInvitations).toHaveBeenCalledTimes(2));
  });

  it("should refetch the invitations of the current workspace after cancelling an invitation", async () => {
    const workspaceInvitations = [buildInvitation("invitation-workspace", ScopeType.workspace, WORKSPACE_A.scopeId)];

    mockInvitationsByScopeId({ [WORKSPACE_A.scopeId]: workspaceInvitations });
    mockCancelUserInvitation.mockResolvedValue(workspaceInvitations[0]);

    const { result } = renderHook(
      () => ({ invitations: useListUserInvitations(WORKSPACE_A), cancel: useCancelUserInvitation() }),
      { wrapper: createWrapper(queryClient) }
    );

    await waitFor(() => expect(result.current.invitations).toEqual(workspaceInvitations));

    await result.current.cancel.mutateAsync({ id: workspaceInvitations[0].id });

    await waitFor(() => expect(mockListPendingInvitations).toHaveBeenCalledTimes(2));
  });

  it("should use the workspace scope key for workspace invitations", async () => {
    const workspaceInvitations = [buildInvitation("invitation-workspace", ScopeType.workspace, WORKSPACE_A.scopeId)];

    mockInvitationsByScopeId({ [WORKSPACE_A.scopeId]: workspaceInvitations });

    renderHook(() => useListUserInvitations(WORKSPACE_A), { wrapper: createWrapper(queryClient) });

    await waitFor(() =>
      expect(queryClient.getQueryData([SCOPE_WORKSPACE, "userInvitations", WORKSPACE_A.scopeId])).toEqual(
        workspaceInvitations
      )
    );
  });
});
