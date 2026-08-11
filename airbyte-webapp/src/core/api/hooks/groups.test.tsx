import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import {
  groupKeys,
  useAddGroupMember,
  useCreateGroup,
  useDeleteGroup,
  useGetGroup,
  useListGroupMembers,
  useListGroups,
  useRemoveGroupMember,
  useUpdateGroup,
} from "./groups";
import { HttpProblem } from "../errors";
import {
  addGroupMember,
  createGroup,
  deleteGroup,
  getGroup,
  listGroupMembers,
  listGroups,
  removeGroupMember,
  updateGroup,
} from "../generated/AirbyteClient";
import { GroupMemberRead, GroupMemberReadList, GroupRead, GroupReadList } from "../types/AirbyteClient";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  listGroups: jest.fn(),
  getGroup: jest.fn(),
  listGroupMembers: jest.fn(),
  createGroup: jest.fn(),
  updateGroup: jest.fn(),
  deleteGroup: jest.fn(),
  addGroupMember: jest.fn(),
  removeGroupMember: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockRegisterNotification = jest.fn();
jest.mock("core/services/Notification", () => ({
  useNotificationService: jest.fn(() => ({ registerNotification: mockRegisterNotification })),
}));

jest.mock("react-intl", () => ({
  useIntl: jest.fn(() => ({ formatMessage: ({ id }: { id: string }) => id })),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockListGroups = listGroups as jest.MockedFunction<typeof listGroups>;
const mockGetGroup = getGroup as jest.MockedFunction<typeof getGroup>;
const mockListGroupMembers = listGroupMembers as jest.MockedFunction<typeof listGroupMembers>;
const mockCreateGroup = createGroup as jest.MockedFunction<typeof createGroup>;
const mockUpdateGroup = updateGroup as jest.MockedFunction<typeof updateGroup>;
const mockDeleteGroup = deleteGroup as jest.MockedFunction<typeof deleteGroup>;
const mockAddGroupMember = addGroupMember as jest.MockedFunction<typeof addGroupMember>;
const mockRemoveGroupMember = removeGroupMember as jest.MockedFunction<typeof removeGroupMember>;

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

const request = { method: "POST" as const, url: "/api/v1/groups" };

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

      await waitFor(() => expect(result.current).toEqual(baseGroupList));
      expect(mockListGroups).toHaveBeenCalledWith({ organizationId }, {});
    });

    it("does not call listGroups when no organization id is available", () => {
      mockUseCurrentOrganizationId.mockReturnValue(undefined as unknown as string);

      renderHook(() => useListGroups(), { wrapper });

      expect(mockListGroups).not.toHaveBeenCalled();
    });
  });

  describe("useGetGroup", () => {
    it("calls getGroup({ groupId }) and returns the group", async () => {
      mockGetGroup.mockResolvedValue(baseGroup);

      const { result } = renderHook(() => useGetGroup(groupId), { wrapper });

      await waitFor(() => expect(result.current).toEqual(baseGroup));
      expect(mockGetGroup).toHaveBeenCalledWith({ groupId }, {});
    });
  });

  describe("useListGroupMembers", () => {
    it("calls listGroupMembers({ groupId }) and returns the member list", async () => {
      mockListGroupMembers.mockResolvedValue(baseMemberList);

      const { result } = renderHook(() => useListGroupMembers(groupId), { wrapper });

      await waitFor(() => expect(result.current).toEqual(baseMemberList));
      expect(mockListGroupMembers).toHaveBeenCalledWith({ groupId }, {});
    });

    it("returns an empty member list when the group has no members", async () => {
      mockListGroupMembers.mockResolvedValue({ members: [] });

      const { result } = renderHook(() => useListGroupMembers(groupId), { wrapper });

      await waitFor(() => expect(result.current).toEqual({ members: [] }));
    });
  });

  describe("useCreateGroup", () => {
    it("calls createGroup with the current organizationId, shows a success toast, and invalidates the group list", async () => {
      mockCreateGroup.mockResolvedValue(baseGroup);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useCreateGroup(), { wrapper });
      await result.current.mutateAsync({ name: "Data team" });

      expect(mockCreateGroup).toHaveBeenCalledWith({ name: "Data team", organizationId }, {});
      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({ type: "success", id: "settings.organization.groups.create.success" })
      );
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.list(organizationId));
    });

    it("shows the generic error toast when createGroup fails with an unrecognized error", async () => {
      mockCreateGroup.mockRejectedValue(new Error("network down"));

      const { result } = renderHook(() => useCreateGroup(), { wrapper });
      await expect(result.current.mutateAsync({ name: "Data team" })).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          id: "settings.organization.groups.create.error",
          text: "settings.organization.groups.create.error",
        })
      );
    });

    it("surfaces the server message when createGroup fails validation with a bad-request problem", async () => {
      const problem = new HttpProblem(request, 400, {
        type: "https://reference.airbyte.com/reference/errors#bad-request",
        title: "bad-request",
        data: { message: "Group name must not be blank." },
      });
      mockCreateGroup.mockRejectedValue(problem);

      const { result } = renderHook(() => useCreateGroup(), { wrapper });
      await expect(result.current.mutateAsync({ name: " " })).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          id: "settings.organization.groups.create.error",
          text: "Group name must not be blank.",
        })
      );
    });

    it("surfaces the server message when createGroup fails with error:group-already-exists", async () => {
      const problem = new HttpProblem(request, 409, {
        type: "error:group-already-exists",
        title: "Group already exists",
        data: { message: "A group named Data team already exists in this organization." },
      });
      mockCreateGroup.mockRejectedValue(problem);

      const { result } = renderHook(() => useCreateGroup(), { wrapper });
      await expect(result.current.mutateAsync({ name: "Data team" })).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          id: "settings.organization.groups.create.error",
          text: "A group named Data team already exists in this organization.",
        })
      );
    });
  });

  describe("useUpdateGroup", () => {
    it("invalidates both the group list and the group detail on success", async () => {
      mockUpdateGroup.mockResolvedValue(baseGroup);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useUpdateGroup(), { wrapper });
      await result.current.mutateAsync({ groupId, name: "Renamed team" });

      expect(mockUpdateGroup).toHaveBeenCalledWith({ groupId, name: "Renamed team" }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.list(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.detail(groupId));
    });
  });

  describe("useDeleteGroup", () => {
    it("invalidates both the group list and the group detail on success", async () => {
      mockDeleteGroup.mockResolvedValue(undefined);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useDeleteGroup(), { wrapper });
      await result.current.mutateAsync(groupId);

      expect(mockDeleteGroup).toHaveBeenCalledWith({ groupId }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.list(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.detail(groupId));
    });

    it("shows the server message when SCIM manages the group", async () => {
      const problem = new HttpProblem(request, 409, {
        type: "error:group-managed-by-scim",
        title: "Group managed by SCIM",
        data: { message: "This group is managed by SCIM and cannot be deleted." },
      });
      mockDeleteGroup.mockRejectedValue(problem);

      const { result } = renderHook(() => useDeleteGroup(), { wrapper });
      await expect(result.current.mutateAsync(groupId)).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          text: "This group is managed by SCIM and cannot be deleted.",
        })
      );
    });
  });

  describe("useAddGroupMember", () => {
    it("invalidates the member list and the group list (memberCount) on success", async () => {
      mockAddGroupMember.mockResolvedValue(baseMember);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useAddGroupMember(), { wrapper });
      await result.current.mutateAsync({ groupId, userId: "user-123" });

      expect(mockAddGroupMember).toHaveBeenCalledWith({ groupId, userId: "user-123" }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.memberList(groupId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.list(organizationId));
    });

    it("shows the server message from a state-conflict response (the user is inactive)", async () => {
      const problem = new HttpProblem(request, 409, {
        type: "https://reference.airbyte.com/reference/errors#409-state-conflict",
        title: "state-conflict",
        data: { message: "This user is inactive and cannot be added to a group." },
      });
      mockAddGroupMember.mockRejectedValue(problem);

      const { result } = renderHook(() => useAddGroupMember(), { wrapper });
      await expect(result.current.mutateAsync({ groupId, userId: "user-123" })).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          text: "This user is inactive and cannot be added to a group.",
        })
      );
    });

    it("reads the message from `detail` (not `data.message`) for a resource-not-found response", async () => {
      // `detail` is typed by Orval as the literal example string from the OpenAPI spec, not a free-form
      // string, so this test uses that exact literal — the point under test is that the hook reads
      // `detail` (not `data.message`, which `ProblemResourceData` doesn't have) for this problem type.
      const problem = new HttpProblem(request, 404, {
        type: "https://reference.airbyte.com/reference/errors#resource-not-found",
        title: "resource-not-found",
        detail: "The requested resource could not be found.",
        data: { resourceType: "user", resourceId: "user-123" },
      });
      mockAddGroupMember.mockRejectedValue(problem);

      const { result } = renderHook(() => useAddGroupMember(), { wrapper });
      await expect(result.current.mutateAsync({ groupId, userId: "user-123" })).rejects.toThrow();

      expect(mockRegisterNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "error",
          text: "The requested resource could not be found.",
        })
      );
    });
  });

  describe("useRemoveGroupMember", () => {
    it("invalidates the member list and the group list (memberCount) on success", async () => {
      mockRemoveGroupMember.mockResolvedValue(undefined);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useRemoveGroupMember(), { wrapper });
      await result.current.mutateAsync({ groupId, userId: "user-123" });

      expect(mockRemoveGroupMember).toHaveBeenCalledWith({ groupId, userId: "user-123" }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.memberList(groupId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(groupKeys.list(organizationId));
    });
  });
});
