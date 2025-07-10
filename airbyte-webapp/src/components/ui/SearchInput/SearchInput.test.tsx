import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { SearchInput } from "./SearchInput";

describe("SearchInput", () => {
  it("calls onChange with the new value when input changes", async () => {
    const mockOnChange = jest.fn();

    await render(<SearchInput value="default" onChange={mockOnChange} />);

    const input = screen.getByRole("search");
    await userEvent.clear(input);
    await userEvent.type(input, "new search value");

    await waitFor(() => expect(mockOnChange).toHaveBeenCalledWith("new search value"));
  });

  it("debounce prevents onChange from being called more than once", async () => {
    const mockOnChange = jest.fn();

    await render(<SearchInput value="default" onChange={mockOnChange} debounceTimeout={300} />);

    const input = screen.getByRole("search");
    await userEvent.clear(input);
    await userEvent.type(input, "new search value", { delay: 50 });

    await waitFor(() => expect(mockOnChange).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(mockOnChange).toHaveBeenNthCalledWith(1, "new search value"));
  });
});
