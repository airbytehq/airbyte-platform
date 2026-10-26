import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SegmentedControl } from "./SegmentedControl";

const options = [
  { label: "First", value: "first" },
  { label: "Second", value: "second" },
];

describe("SegmentedControl", () => {
  it("checks no segment when selectedValue is undefined", () => {
    render(<SegmentedControl options={options} onSelect={jest.fn()} aria-label="Choice" />);

    expect(screen.getAllByRole("radio")).toHaveLength(2);
    screen.getAllByRole("radio").forEach((radio) => expect(radio).toHaveAttribute("aria-checked", "false"));
  });

  it("calls onSelect once with the clicked value", async () => {
    const onSelect = jest.fn();
    render(<SegmentedControl options={options} selectedValue="first" onSelect={onSelect} aria-label="Choice" />);

    await userEvent.click(screen.getByRole("radio", { name: "Second" }));

    expect(onSelect).toHaveBeenCalledTimes(1);
    expect(onSelect).toHaveBeenCalledWith("second");
  });
});
