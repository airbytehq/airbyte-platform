import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { LevelValue } from "./LevelValue";

describe("LevelValue", () => {
  it("shows the value", async () => {
    await render(<LevelValue level="success">Store X</LevelValue>);

    expect(screen.getByText("Store X")).toBeInTheDocument();
  });

  it("shows the value for every level", async () => {
    await render(
      <>
        <LevelValue level="success">Passed</LevelValue>
        <LevelValue level="warning">Incomplete</LevelValue>
        <LevelValue level="error">Failed</LevelValue>
        <LevelValue level="neutral">Unknown</LevelValue>
      </>
    );

    expect(["Passed", "Incomplete", "Failed", "Unknown"].map((text) => screen.getByText(text))).toHaveLength(4);
  });
});
