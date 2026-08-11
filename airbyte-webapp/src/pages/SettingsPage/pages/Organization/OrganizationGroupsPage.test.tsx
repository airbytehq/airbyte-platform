import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { OrganizationGroupsPage } from "./OrganizationGroupsPage";

describe("OrganizationGroupsPage", () => {
  it("renders the user groups heading", async () => {
    await render(<OrganizationGroupsPage />);

    expect(screen.getByRole("heading", { name: "User Groups" })).toBeInTheDocument();
  });
});
