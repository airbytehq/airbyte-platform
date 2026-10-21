import { render, screen } from "@testing-library/react";

import { useIsCloudApp } from "core/utils/app";

import { CloudInviteUsersButton } from "./CloudInviteUsersButton";

jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("cloud/views/users/InviteUsersButton", () => ({
  InviteUsersButton: () => <button type="button">Invite users</button>,
}));

describe("CloudInviteUsersButton", () => {
  it("shows the invite button in Cloud", async () => {
    jest.mocked(useIsCloudApp).mockReturnValue(true);

    render(<CloudInviteUsersButton />);

    expect(await screen.findByRole("button", { name: "Invite users" })).toBeInTheDocument();
  });

  it("shows nothing outside Cloud", () => {
    jest.mocked(useIsCloudApp).mockReturnValue(false);

    const { container } = render(<CloudInviteUsersButton />);

    expect(container).toBeEmptyDOMElement();
  });
});
