import { render, screen } from "@testing-library/react";

import { useIsCloudApp } from "core/utils/app";

import { CloudInviteUsersButton } from "./CloudInviteUsersButton";

jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("cloud/views/users/InviteUsersButton", () => ({
  InviteUsersButton: ({ disabled }: { disabled?: boolean }) => (
    <button type="button" disabled={disabled}>
      Invite users
    </button>
  ),
}));

describe("CloudInviteUsersButton", () => {
  it("shows the invite button in Cloud", async () => {
    jest.mocked(useIsCloudApp).mockReturnValue(true);

    render(<CloudInviteUsersButton />);

    expect(await screen.findByRole("button", { name: "Invite users" })).toBeInTheDocument();
  });

  it("passes disabled to the invite button", async () => {
    jest.mocked(useIsCloudApp).mockReturnValue(true);

    render(<CloudInviteUsersButton disabled />);

    expect(await screen.findByRole("button", { name: "Invite users" })).toBeDisabled();
  });

  it("shows nothing outside Cloud", () => {
    jest.mocked(useIsCloudApp).mockReturnValue(false);

    const { container } = render(<CloudInviteUsersButton />);

    expect(container).toBeEmptyDOMElement();
  });
});
