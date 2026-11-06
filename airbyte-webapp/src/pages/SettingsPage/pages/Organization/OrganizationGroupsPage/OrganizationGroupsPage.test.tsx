import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { OrganizationGroupsPage } from "./OrganizationGroupsPage";

// Stubbed so this file asserts the page shell and its delegation only. The list's own search,
// card, and empty-state branches are covered in GroupsList.test.tsx.
jest.mock("./components/GroupsList", () => ({
  GroupsList: () => <div data-testid="groups-list" />,
}));

describe("OrganizationGroupsPage", () => {
  it("renders the user groups heading", async () => {
    await render(<OrganizationGroupsPage />);

    expect(screen.getByRole("heading", { name: "User Groups" })).toBeInTheDocument();
  });

  it("renders the groups list below the heading", async () => {
    await render(<OrganizationGroupsPage />);

    expect(screen.getByTestId("groups-list")).toBeInTheDocument();
  });
});
