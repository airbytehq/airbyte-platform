import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useDbtCloudServiceToken } from "core/api/cloud";
import { useGeneratedIntent } from "core/utils/rbac";

import { DbtCloudSettingsView } from "./DbtCloudSettingsView";

jest.mock("core/api/cloud", () => ({ useDbtCloudServiceToken: jest.fn() }));
jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));
jest.mock("./useDbtTokenRemovalModal", () => ({ useDbtTokenRemovalModal: () => jest.fn() }));

describe("DbtCloudSettingsView", () => {
  const saveToken = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    saveToken.mockResolvedValue({});
    mocked(useGeneratedIntent).mockReturnValue(true);
    mocked(useDbtCloudServiceToken).mockReturnValue({
      hasExistingToken: true,
      accessUrl: undefined,
      saveToken,
      isSavingToken: false,
      deleteToken: jest.fn(),
      isDeletingToken: false,
    });
  });

  it("lets the user replace an existing token without deleting the integration", async () => {
    await render(<DbtCloudSettingsView />);

    await userEvent.type(screen.getByLabelText("Service Token"), "replacement-token");
    await userEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() =>
      expect(saveToken).toHaveBeenCalledWith({ authToken: "replacement-token", accessUrl: undefined })
    );
    expect(screen.getByRole("button", { name: "Delete dbt Cloud configuration" })).toBeInTheDocument();
  });

  it("prevents viewers from replacing the token", async () => {
    mocked(useGeneratedIntent).mockReturnValue(false);
    await render(<DbtCloudSettingsView />);

    expect(screen.getByLabelText("Service Token")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Save changes" })).toBeDisabled();
  });

  it("keeps the existing custom URL when only the token is replaced", async () => {
    mocked(useDbtCloudServiceToken).mockReturnValue({
      hasExistingToken: true,
      accessUrl: "custom.getdbt.com",
      saveToken,
      isSavingToken: false,
      deleteToken: jest.fn(),
      isDeletingToken: false,
    });
    await render(<DbtCloudSettingsView />);

    expect(screen.getByLabelText(/dbt Access URL/)).toHaveValue("custom.getdbt.com");
    expect(screen.getByLabelText(/dbt Access URL/)).toBeDisabled();
    await userEvent.type(screen.getByLabelText("Service Token"), "replacement-token");
    await userEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() =>
      expect(saveToken).toHaveBeenCalledWith({ authToken: "replacement-token", accessUrl: "custom.getdbt.com" })
    );
  });
});
