import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { Age } from "./Age";
import { formatAge, formatDuration } from "../../utils";

describe("Age", () => {
  it("shows how long ago something happened", async () => {
    await render(<Age age={formatAge(72_000)} />);

    expect(screen.getByText("1m 12s ago")).toBeInTheDocument();
  });

  it("shows how long something has lasted, without the ago", async () => {
    await render(<Age age={formatDuration(72_000)} />);

    expect(screen.getByText("1m 12s")).toBeInTheDocument();
  });
});
