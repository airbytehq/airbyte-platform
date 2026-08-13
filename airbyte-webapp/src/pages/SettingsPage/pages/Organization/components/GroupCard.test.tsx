import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { useListGroupMembers } from "core/api";
import { GroupMemberReadList, GroupRead } from "core/api/types/AirbyteClient";

import { GroupCard } from "./GroupCard";

// Asserted at the hook boundary rather than on the generated client: `import/no-restricted-paths`
// forbids importing core/api/generated outside core/api. That the hook issues no request while
// `enabled` is false is covered directly in core/api/hooks/groups.test.tsx.
jest.mock("core/api", () => ({
  useListGroupMembers: jest.fn(),
}));

const mockUseListGroupMembers = useListGroupMembers as jest.Mock;

const group: GroupRead = {
  groupId: "group-1",
  name: "Data team",
  description: "Analysts and data scientists",
  organizationId: "org-1",
  memberCount: 2,
};

const emptyGroup: GroupRead = { ...group, groupId: "group-2", name: "Empty team", memberCount: 0 };

// Listed deliberately out of name order: listGroupMembers applies no sort server-side.
const memberList: GroupMemberReadList = {
  members: [
    { memberId: "m-1", groupId: group.groupId, userId: "u-1", userEmail: "zoe@example.com", userName: "Zoe Alpha" },
    { memberId: "m-2", groupId: group.groupId, userId: "u-2", userEmail: "adam@example.com", userName: "Adam Beta" },
  ],
};

const setMemberQuery = (state: { data?: GroupMemberReadList; isInitialLoading?: boolean; isError?: boolean }) =>
  mockUseListGroupMembers.mockReturnValue({
    data: undefined,
    isInitialLoading: false,
    isError: false,
    ...state,
  });

/** The `enabled` value the card passed on its most recent render. */
const lastEnabled = (): boolean => {
  const { calls } = mockUseListGroupMembers.mock;
  return calls[calls.length - 1][1].enabled;
};

const expandCard = () => userEvent.click(screen.getByRole("button", { name: `View members of ${group.name}` }));

describe(`${GroupCard.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    setMemberQuery({ data: memberList });
  });

  it("renders the member count inside the group name, and the description beneath", async () => {
    await render(<GroupCard group={group} />);

    expect(screen.getByText("Data team (2)")).toBeInTheDocument();
    expect(screen.getByText("Analysts and data scientists")).toBeInTheDocument();
  });

  it("does not render an add-members control", async () => {
    await render(<GroupCard group={group} />);

    expect(screen.queryByRole("button", { name: "Add members" })).not.toBeInTheDocument();
  });

  it("keeps the member query disabled until the card is expanded", async () => {
    await render(<GroupCard group={group} />);

    expect(mockUseListGroupMembers).toHaveBeenCalledWith(group.groupId, { enabled: false });
    expect(lastEnabled()).toBe(false);
  });

  it("enables the member query once the card is expanded", async () => {
    await render(<GroupCard group={group} />);

    await expandCard();

    await waitFor(() => expect(lastEnabled()).toBe(true));
    expect(mockUseListGroupMembers).toHaveBeenCalledWith(group.groupId, { enabled: true });
  });

  it("sorts the expanded member list by name", async () => {
    await render(<GroupCard group={group} />);

    await expandCard();

    const renderedNames = screen.getAllByText(/^(Adam Beta|Zoe Alpha)$/).map((element) => element.textContent);
    expect(renderedNames).toEqual(["Adam Beta", "Zoe Alpha"]);
  });

  it("shows the member email beneath each member name", async () => {
    await render(<GroupCard group={group} />);

    await expandCard();

    expect(screen.getByText("adam@example.com")).toBeInTheDocument();
    expect(screen.getByText("zoe@example.com")).toBeInTheDocument();
  });

  it("renders an inline error when the member fetch fails", async () => {
    setMemberQuery({ isError: true });

    await render(<GroupCard group={group} />);

    await expandCard();

    expect(screen.getByText("Something went wrong loading members. Please try again.")).toBeInTheDocument();
  });

  it("renders an empty-members message, and no rows, when members were removed after the list was fetched", async () => {
    // group.memberCount is a list-time snapshot; the member fetch can still resolve empty if the
    // group emptied out before expansion.
    setMemberQuery({ data: { members: [] } });

    await render(<GroupCard group={group} />);

    await expandCard();

    expect(screen.getByText("This group has no members.")).toBeInTheDocument();
    expect(screen.queryByText("adam@example.com")).not.toBeInTheDocument();
    expect(screen.queryByText("zoe@example.com")).not.toBeInTheDocument();
  });

  it("does not render a row actions menu", async () => {
    await render(<GroupCard group={group} />);

    expect(screen.queryByTestId("group-actions-menu")).not.toBeInTheDocument();
  });

  describe("when the group has no members", () => {
    it("disables the view-members control", async () => {
      await render(<GroupCard group={emptyGroup} />);

      expect(screen.getByRole("button", { name: `View members of ${emptyGroup.name}` })).toBeDisabled();
    });

    it("never mounts the member list, so the member query is never called", async () => {
      await render(<GroupCard group={emptyGroup} />);

      expect(mockUseListGroupMembers).not.toHaveBeenCalled();
    });

    it("does not render an add-members control", async () => {
      await render(<GroupCard group={emptyGroup} />);

      expect(screen.queryByRole("button", { name: "Add members" })).not.toBeInTheDocument();
    });
  });

  it("gives each card's view-members control a distinct accessible name, for screen-reader buttons lists", async () => {
    await render(
      <>
        <GroupCard group={group} />
        <GroupCard group={emptyGroup} />
      </>
    );

    expect(screen.getByRole("button", { name: `View members of ${group.name}` })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: `View members of ${emptyGroup.name}` })).toBeInTheDocument();
  });
});
