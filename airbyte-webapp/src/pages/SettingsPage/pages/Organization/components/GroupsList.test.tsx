import { act, render as rtlRender, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render, TestWrapper } from "test-utils";

import { useCurrentOrganizationInfo, useListGroups } from "core/api";
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
  useCurrentOrganizationInfo: jest.fn(),
}));

const mockUseListGroups = useListGroups as jest.Mock;
const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.Mock;

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

const SCIM_BANNER =
  "User groups are managed by your identity provider. Add, edit, and delete groups there, not in Airbyte.";
const ENABLE_SCIM_BANNER = "User groups are managed by your identity provider. Enable SCIM to configure user groups.";

const setGroups = (groups: GroupRead[]) =>
  mockUseListGroups.mockReturnValue({ data: { groups }, isInitialLoading: false, isError: false });

const setListQuery = (state: { isInitialLoading?: boolean; isError?: boolean }) =>
  mockUseListGroups.mockReturnValue({ data: undefined, isInitialLoading: false, isError: false, ...state });

const setOrganizationInfo = (info: { sso: boolean; scim: boolean }) =>
  mockUseCurrentOrganizationInfo.mockReturnValue({ organizationId, organizationName: "Org", ...info });

const searchFor = async (term: string) => {
  await userEvent.type(screen.getByPlaceholderText("Search for group"), term);
};

describe(`${GroupsList.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    setGroups([dataTeam, itTeam]);
    setOrganizationInfo({ sso: false, scim: false });
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

  describe("when SCIM is enabled", () => {
    beforeEach(() => setOrganizationInfo({ sso: false, scim: true }));

    it("renders the identity-provider banner with the exact copy", async () => {
      await render(<GroupsList />);

      expect(screen.getByText(SCIM_BANNER)).toBeInTheDocument();
    });

    it("does not render the enable-SCIM banner copy", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText(ENABLE_SCIM_BANNER)).not.toBeInTheDocument();
    });

    it("renders the SCIM Enabled chip", async () => {
      await render(<GroupsList />);

      expect(screen.getByText("SCIM Enabled")).toBeInTheDocument();
    });

    it("renders the SSO Enabled chip when SSO is also enabled", async () => {
      setOrganizationInfo({ sso: true, scim: true });

      await render(<GroupsList />);

      expect(screen.getByText("SSO Enabled")).toBeInTheDocument();
    });

    it("does not render the SSO Enabled chip when SSO is disabled", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText("SSO Enabled")).not.toBeInTheDocument();
    });

    it("renders the banner while the group list is still loading", async () => {
      setListQuery({ isInitialLoading: true });

      await render(<GroupsList />);

      expect(screen.getByText(SCIM_BANNER)).toBeInTheDocument();
    });

    it("renders the banner when the group list failed to load", async () => {
      setListQuery({ isError: true });

      await render(<GroupsList />);

      expect(screen.getByText(SCIM_BANNER)).toBeInTheDocument();
    });
  });

  describe("when SCIM is disabled and groups are present", () => {
    // Covered by the top-level beforeEach: setGroups([dataTeam, itTeam]) and
    // setOrganizationInfo({ sso: false, scim: false }).

    it("renders the enable-SCIM banner with the exact copy", async () => {
      await render(<GroupsList />);

      expect(screen.getByText(ENABLE_SCIM_BANNER)).toBeInTheDocument();
    });

    it("does not render the identity-provider banner copy", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
    });

    it("does not render the SCIM Enabled chip", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText("SCIM Enabled")).not.toBeInTheDocument();
    });

    it("keeps the banner after a search that matches no group", async () => {
      await render(<GroupsList />);

      await searchFor("zzz");

      await waitFor(() => expect(screen.getByText(NO_MATCHES_TEXT)).toBeInTheDocument());
      expect(screen.getByText(ENABLE_SCIM_BANNER)).toBeInTheDocument();
    });
  });

  describe("when SCIM is disabled and there are no groups", () => {
    beforeEach(() => setGroups([]));

    it("renders neither banner copy", async () => {
      await render(<GroupsList />);

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
      expect(screen.queryByText(ENABLE_SCIM_BANNER)).not.toBeInTheDocument();
    });
  });

  describe("when SCIM is disabled and the group list state is undecided", () => {
    it("renders neither banner copy while the group list is still loading", async () => {
      setListQuery({ isInitialLoading: true });

      await render(<GroupsList />);

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
      expect(screen.queryByText(ENABLE_SCIM_BANNER)).not.toBeInTheDocument();
    });

    it("renders neither banner copy when the group list failed to load", async () => {
      setListQuery({ isError: true });

      await render(<GroupsList />);

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
      expect(screen.queryByText(ENABLE_SCIM_BANNER)).not.toBeInTheDocument();
    });
  });

  it("renders neither banner nor either chip, and does not throw, when organization info is undefined", async () => {
    mockUseCurrentOrganizationInfo.mockReturnValue(undefined);
    // scim/sso both read as undefined (falsy) with no org info, which is the same falsy value
    // state (b) exercises - so with groups present the enable-SCIM banner would correctly render
    // via the isReady && groups.length > 0 fallback (already covered by the state-(b) tests
    // above). Empty groups isolate what this test is actually checking: the undefined-org-info
    // case itself doesn't crash and doesn't turn on either chip.
    setGroups([]);

    // Rendering itself is the "does not throw" assertion: an unhandled error here fails the test
    // before any of the following queries run.
    await render(<GroupsList />);

    expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
    expect(screen.queryByText(ENABLE_SCIM_BANNER)).not.toBeInTheDocument();
    expect(screen.queryByText("SSO Enabled")).not.toBeInTheDocument();
    expect(screen.queryByText("SCIM Enabled")).not.toBeInTheDocument();
  });

  it("does not lock the group cards when SCIM is enabled: 'View members' stays available for a group with members", async () => {
    setOrganizationInfo({ sso: false, scim: true });

    await render(<GroupsList />);

    expect(screen.getByText("Data team (10)")).toBeInTheDocument();

    const viewMembersControl = screen.getByRole("button", { name: "View members of Data team" });
    expect(viewMembersControl).toBeInTheDocument();
    expect(viewMembersControl).toBeEnabled();
  });
});
