import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { trackError } from "core/utils/datadog";

import { InstallMcpScreen } from "./InstallMcpScreen";

const mockError = new Error("spec request failed");

jest.mock("core/utils/datadog", () => ({ trackError: jest.fn() }));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: () => "workspace-1" }));
jest.mock("./useSuggestedPair", () => ({
  useSuggestedPair: () => {
    throw mockError;
  },
}));

describe("InstallMcpScreen", () => {
  it("shows the general prompt and reports the error when the pair lookup fails", async () => {
    const consoleError = jest.spyOn(console, "error").mockImplementation(() => undefined);

    await render(<InstallMcpScreen sourcePicks={["source-1"]} destinationChoice="bigquery" />);

    expect(await screen.findByText(/Help me set up my first Airbyte connection/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy prompt" })).toBeInTheDocument();
    expect(mocked(trackError)).toHaveBeenCalledWith(mockError);
    consoleError.mockRestore();
  });
});
