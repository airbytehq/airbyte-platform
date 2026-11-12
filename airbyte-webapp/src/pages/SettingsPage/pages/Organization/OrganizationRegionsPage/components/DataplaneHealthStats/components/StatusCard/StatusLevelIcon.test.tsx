import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { StatusLevel, StatusLevelIcon } from "./StatusLevelIcon";

jest.mock("components/ui/Icon", () => ({
  Icon: ({ type, color }: { type: string; color: string }) => (
    <span data-testid="icon" data-type={type} data-color={color} />
  ),
}));

const iconPropsFor = async (level: StatusLevel) => {
  await render(<StatusLevelIcon level={level} />);
  const { type, color } = screen.getByTestId("icon").dataset;
  return { type, color };
};

describe("StatusLevelIcon", () => {
  it("uses the green check for the success level", async () => {
    expect(await iconPropsFor("success")).toEqual({ type: "statusSuccess", color: "success" });
  });

  it("uses the amber warning for the warning level", async () => {
    expect(await iconPropsFor("warning")).toEqual({ type: "statusWarning", color: "warning" });
  });

  it("uses the red cross for the error level", async () => {
    expect(await iconPropsFor("error")).toEqual({ type: "statusError", color: "error" });
  });

  it("uses the grey cross for the neutral level", async () => {
    expect(await iconPropsFor("neutral")).toEqual({ type: "statusError", color: "disabled" });
  });
});
