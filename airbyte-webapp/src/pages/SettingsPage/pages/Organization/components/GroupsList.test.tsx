import { act, render as rtlRender, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render, TestWrapper } from "test-utils";

import { useListGroups } from "core/api";
import { GroupRead } from "core/api/types/AirbyteClient";

import { GroupsList } from "./GroupsList";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as OrganizationAccessManagementSection.test.tsx) - list the exports this
// component uses explicitly.
// useListGroupMembers is reached through the cards this list renders. Its own behaviour is covered
// in GroupCard.test.tsx, so here it only needs to be a function.
jest.mock("core/api", () => ({
  useListGroups: jest.fn(),
  useListGroupMembers: jest.fn(() => ({ data: undefined, isInitialLoading: false, isError: false })),
}));

const mockUseListGroups = useListGroups as jest.Mock;

const organizationId = "org-1";

const dataTeam: GroupRead = {
  groupId: "group-1",
  name: "Data team",
  description: "Analysts and data scientists",
  organizationId,
  memberCount: 10,
};

const itTeam: GroupRead = {
  groupId: "group-2",
  name: "IT team",
  description: "Infrastructure and support",
  organizationId,
  memberCount: 5,
};

const EMPTY_STATE_TEXT = "No user groups yet";
const NO_MATCHES_TEXT = "No matching groups found";

const setGroups = (groups: GroupRead[]) =>
  mockUseListGroups.mockReturnValue({ data: { groups }, isInitialLoading: false, isError: false });

const setListQuery = (state: { isInitialLoading?: boolean; isError?: boolean }) =>
  mockUseListGroups.mockReturnValue({ data: undefined, isInitialLoading: false, isError: false, ...state });

const searchFor = async (term: string) => {
  await userEvent.type(screen.getByPlaceholderText("Search for group"), term);
};

describe(`${GroupsList.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    setGroups([dataTeam, itTeam]);
  });

  it("renders one card per group, with the member count inside the group name", async () => {
    await render(<GroupsList />);

    expect(screen.getByText("Data team (10)")).toBeInTheDocument();
    expect(screen.getByText("IT team (5)")).toBeInTheDocument();
  });

  it("renders each group description", async () => {
    await render(<GroupsList />);

    expect(screen.getByText("Analysts and data scientists")).toBeInTheDocument();
    expect(screen.getByText("Infrastructure and support")).toBeInTheDocument();
  });

  it("does not render an add-group control", async () => {
    await render(<GroupsList />);

    expect(screen.queryByRole("button", { name: "Add group" })).not.toBeInTheDocument();
  });

  it("narrows the list to the groups whose name matches the search term", async () => {
    await render(<GroupsList />);

    await searchFor("data");

    await waitFor(() => expect(screen.queryByText("IT team (5)")).not.toBeInTheDocument());
    expect(screen.getByText("Data team (10)")).toBeInTheDocument();
  });

  it("narrows the list to the groups whose description matches the search term", async () => {
    await render(<GroupsList />);

    await searchFor("infrastructure");

    await waitFor(() => expect(screen.queryByText("Data team (10)")).not.toBeInTheDocument());
    expect(screen.getByText("IT team (5)")).toBeInTheDocument();
  });

  it("applies the filter from a shared or reloaded URL", async () => {
    // The shared render helper does not take a route, so wrap manually with one preset.
    await act(async () => {
      rtlRender(
        <TestWrapper route="/?search=infrastructure">
          <GroupsList />
        </TestWrapper>
      );
    });

    expect(screen.getByPlaceholderText("Search for group")).toHaveValue("infrastructure");
    expect(screen.getByText("IT team (5)")).toBeInTheDocument();
    expect(screen.queryByText("Data team (10)")).not.toBeInTheDocument();
  });

  it("shows the no-matches line, and not the empty state, when the search matches nothing", async () => {
    await render(<GroupsList />);

    await searchFor("zzz");

    await waitFor(() => expect(screen.getByText(NO_MATCHES_TEXT)).toBeInTheDocument());
    expect(screen.queryByText(EMPTY_STATE_TEXT)).not.toBeInTheDocument();
    expect(screen.queryByText("Data team (10)")).not.toBeInTheDocument();
  });

  it("shows the empty state, and not the no-matches line, when the organization has no groups", async () => {
    setGroups([]);

    await render(<GroupsList />);

    expect(screen.getByText(EMPTY_STATE_TEXT)).toBeInTheDocument();
    expect(screen.getByText("Groups let you manage permissions for several people at once.")).toBeInTheDocument();
    expect(screen.queryByText(NO_MATCHES_TEXT)).not.toBeInTheDocument();
  });

  describe("when the group list fails to load", () => {
    beforeEach(() => setListQuery({ isError: true }));

    it("renders the error inline, on the page", async () => {
      await render(<GroupsList />);

      expect(screen.getByText("Something went wrong loading groups. Please try again.")).toBeInTheDocument();
    });

    it("does not mistake the failure for an empty organization", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText(EMPTY_STATE_TEXT)).not.toBeInTheDocument();
      expect(screen.queryByText(NO_MATCHES_TEXT)).not.toBeInTheDocument();
    });
  });

  it("shows neither empty state while the group list is still loading", async () => {
    setListQuery({ isInitialLoading: true });

    await render(<GroupsList />);

    expect(screen.queryByText(EMPTY_STATE_TEXT)).not.toBeInTheDocument();
    expect(screen.queryByText(NO_MATCHES_TEXT)).not.toBeInTheDocument();
  });
});
