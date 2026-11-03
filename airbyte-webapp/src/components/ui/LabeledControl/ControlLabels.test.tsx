import { screen } from "@testing-library/react";

import { render } from "test-utils/testutils";

import { ControlLabels } from "./ControlLabels";

describe("ControlLabels", () => {
  it("marks a required field with an asterisk hidden from assistive technology", async () => {
    await render(
      <ControlLabels label="Host" htmlFor="host" requirement="required">
        <input id="host" />
      </ControlLabels>
    );

    expect(screen.getByText("*")).toHaveAttribute("aria-hidden", "true");
    expect(screen.queryByText("Optional")).not.toBeInTheDocument();
  });

  it("labels an optional field as Optional without an asterisk", async () => {
    await render(
      <ControlLabels label="Port" htmlFor="port" requirement="optional">
        <input id="port" />
      </ControlLabels>
    );

    expect(screen.getByText("Optional")).toBeInTheDocument();
    expect(screen.queryByText("*")).not.toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: /^Port\s*Optional$/ })).toBeInTheDocument();
  });

  it("shows neither an asterisk nor Optional without a requirement", async () => {
    await render(
      <ControlLabels label="Name" htmlFor="name">
        <input id="name" />
      </ControlLabels>
    );

    expect(screen.queryByText("*")).not.toBeInTheDocument();
    expect(screen.queryByText("Optional")).not.toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
  });
});
