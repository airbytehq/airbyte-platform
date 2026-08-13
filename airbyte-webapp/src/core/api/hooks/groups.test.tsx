import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { useListGroupMembers, useListGroups } from "./groups";
import { listGroupMembers, listGroups } from "../generated/AirbyteClient";
import { GroupMemberRead, GroupMemberReadList, GroupRead, GroupReadList } from "../types/AirbyteClient";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  listGroups: jest.fn(),
  listGroupMembers: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockListGroups = listGroups as jest.MockedFunction<typeof listGroups>;
const mockListGroupMembers = listGroupMembers as jest.MockedFunction<typeof listGroupMembers>;

const organizationId = "org-123";
const groupId = "group-123";

const baseGroup: GroupRead = {
  groupId,
  name: "Data team",
  description: null,
  organizationId,
  memberCount: 2,
};

const baseGroupList: GroupReadList = { groups: [baseGroup] };

const baseMember: GroupMemberRead = {
  memberId: "member-123",
  groupId,
  userId: "user-123",
  userEmail: "user@example.com",
  userName: "User Example",
};

const baseMemberList: GroupMemberReadList = { members: [baseMember] };

describe("groups hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    mockUseCurrentOrganizationId.mockReturnValue(organizationId);
  });

  afterEach(() => {
    queryClient.clear();
  });

  describe("useListGroups", () => {
    it("calls listGroups({ organizationId }) and returns the list", async () => {
      mockListGroups.mockResolvedValue(baseGroupList);

      const { result } = renderHook(() => useListGroups(), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual(baseGroupList));
      expect(mockListGroups).toHaveBeenCalledWith({ organizationId }, {});
    });

    it("does not call listGroups when no organization id is available", () => {
      mockUseCurrentOrganizationId.mockReturnValue(undefined as unknown as string);

      renderHook(() => useListGroups(), { wrapper });

      expect(mockListGroups).not.toHaveBeenCalled();
    });

    it("surfaces the failure on the result rather than throwing, so the caller can render inline", async () => {
      mockListGroups.mockRejectedValue(new Error("forbidden"));

      const { result } = renderHook(() => useListGroups(), { wrapper });

      await waitFor(() => expect(result.current.isError).toBe(true));
      expect(result.current.data).toBeUndefined();
    });
  });

  describe("useListGroupMembers", () => {
    it("calls listGroupMembers({ groupId }) and returns the member list", async () => {
      mockListGroupMembers.mockResolvedValue(baseMemberList);

      const { result } = renderHook(() => useListGroupMembers(groupId), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual(baseMemberList));
      expect(mockListGroupMembers).toHaveBeenCalledWith({ groupId }, {});
    });

    it("returns an empty member list when the group has no members", async () => {
      mockListGroupMembers.mockResolvedValue({ members: [] });

      const { result } = renderHook(() => useListGroupMembers(groupId), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual({ members: [] }));
    });

    it("does not call listGroupMembers while the query is disabled", () => {
      mockListGroupMembers.mockResolvedValue(baseMemberList);

      renderHook(() => useListGroupMembers(groupId, { enabled: false }), { wrapper });

      expect(mockListGroupMembers).not.toHaveBeenCalled();
    });

    it("calls listGroupMembers once the query becomes enabled", async () => {
      mockListGroupMembers.mockResolvedValue(baseMemberList);

      const { result, rerender } = renderHook(({ enabled }) => useListGroupMembers(groupId, { enabled }), {
        wrapper,
        initialProps: { enabled: false },
      });

      expect(mockListGroupMembers).not.toHaveBeenCalled();

      rerender({ enabled: true });

      await waitFor(() => expect(result.current.data).toEqual(baseMemberList));
      expect(mockListGroupMembers).toHaveBeenCalledTimes(1);
    });
  });
});
