import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";

import { TagInput } from "./TagInput";

const TagInputWithWrapper = ({
  onChange,
  uniqueValues,
  itemType = "string",
  initialValues = ["tag1", "tag2"],
}: {
  onChange?: (values: Array<string | number>) => void;
  uniqueValues?: boolean;
  itemType?: "string" | "integer" | "number";
  initialValues?: Array<string | number>;
}) => {
  const [fieldValue, setFieldValue] = useState<Array<string | number>>(initialValues);
  return (
    <TagInput
      name="test"
      fieldValue={fieldValue}
      onChange={(values: Array<string | number>) => {
        onChange?.(values);
        setFieldValue(values);
      }}
      disabled={false}
      uniqueValues={uniqueValues}
      itemType={itemType}
    />
  );
};

describe("<TagInput />", () => {
  it("renders with defaultValue", () => {
    render(<TagInputWithWrapper />);
    const tag1 = screen.getByText("tag1");
    const tag2 = screen.getByText("tag2");
    expect(tag1).toBeInTheDocument();
    expect(tag2).toBeInTheDocument();
  });

  describe("delimiters and keypress events create tags", () => {
    it("adds a tag when user types a tag and hits enter", async () => {
      render(<TagInputWithWrapper />);
      const input = screen.getByRole("combobox");
      await userEvent.type(input, "tag3{enter}");
      const tag3 = screen.getByText("tag3");
      expect(tag3).toBeInTheDocument();
    });
    it("adds a tag when user types a tag and hits tab", async () => {
      render(<TagInputWithWrapper />);
      const input = screen.getByRole("combobox");
      await userEvent.type(input, "tag3{Tab}");
      const tag3 = screen.getByText("tag3");
      expect(tag3).toBeInTheDocument();
    });

    it("adds multiple tags when a user enters a string with commas", async () => {
      render(<TagInputWithWrapper />);
      const input = screen.getByRole("combobox");
      await userEvent.type(input, "tag3, tag4,");
      const tag3 = screen.getByText("tag3");
      expect(tag3).toBeInTheDocument();
      const tag4 = screen.getByText("tag4");
      expect(tag4).toBeInTheDocument();
    });
    it("adds multiple tags when a user enters a string with semicolons", async () => {
      render(<TagInputWithWrapper />);
      const input = screen.getByRole("combobox");
      await userEvent.type(input, "tag3; tag4;");
      const tag3 = screen.getByText("tag3");
      expect(tag3).toBeInTheDocument();
      const tag4 = screen.getByText("tag4");
      expect(tag4).toBeInTheDocument();
    });
    it("handles a combination of methods at once", async () => {
      render(<TagInputWithWrapper />);
      const input = screen.getByRole("combobox");
      await userEvent.type(input, "tag3; tag4{Tab} tag5, tag6{enter}");
      const tag3 = screen.getByText("tag3");
      expect(tag3).toBeInTheDocument();
      const tag4 = screen.getByText("tag4");
      expect(tag4).toBeInTheDocument();
      const tag5 = screen.getByText("tag5");
      expect(tag5).toBeInTheDocument();
      const tag6 = screen.getByText("tag6");
      expect(tag6).toBeInTheDocument();
    });
  });

  it("correctly removes a tag when user clicks its Remove button", async () => {
    render(<TagInputWithWrapper />);
    const tag1 = screen.getByText("tag1");
    expect(tag1).toBeInTheDocument();

    const tag2 = screen.getByText("tag2");
    expect(tag2).toBeInTheDocument();

    const input = screen.getByRole("combobox");
    await userEvent.type(input, "tag3{enter}");
    const tag3 = screen.getByText("tag3");
    expect(tag3).toBeInTheDocument();
    const removeTag2Button = screen.getByRole("button", { name: "Remove tag2" });
    await userEvent.click(removeTag2Button);

    const tag1again = screen.getByText("tag1");
    expect(tag1again).toBeInTheDocument();

    // queryBy because getBy will throw if not in the DOM
    const tag2again = screen.queryByText("tag2");
    expect(tag2again).not.toBeInTheDocument();

    const tag3again = screen.getByText("tag3");
    expect(tag3again).toBeInTheDocument();
  });

  describe("blurring the TagInput", () => {
    it("triggers onChange when the value changes from the blur", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={(values) => mockOnChange(values)} uniqueValues />);
      const input = screen.getByRole("combobox");
      await act(async () => userEvent.type(input, "tag2"));

      mockOnChange.mockClear();
      act(() => input.blur());
      expect(mockOnChange).toHaveBeenCalledWith(["tag1", "tag2"]);
    });

    it("does not trigger onChange when the blurring doesn't result in a new value", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={(values) => mockOnChange(values)} />);
      const input = screen.getByRole("combobox");
      await act(async () => userEvent.type(input, "tag2"));

      mockOnChange.mockClear();
      act(() => input.blur());
      expect(mockOnChange).not.toHaveBeenCalled();
    });
  });

  describe("handling number/integer values", () => {
    it("converts string inputs to integers when itemType is 'integer'", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={mockOnChange} itemType="integer" initialValues={[1, 2]} />);

      const input = screen.getByRole("combobox");
      await userEvent.type(input, "3{enter}");

      // The last call should have the updated array with the new integer
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];
      expect(lastCall).toContain(3);
      // Verify it's a number, not a string
      expect(typeof lastCall[lastCall.length - 1]).toBe("number");
    });

    it("converts string inputs to floats when itemType is 'number'", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={mockOnChange} itemType="number" initialValues={[1.1, 2.2]} />);

      const input = screen.getByRole("combobox");
      await userEvent.type(input, "3.3{enter}");

      // The last call should have the updated array with the new float
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];
      expect(lastCall).toContainEqual(3.3);
      // Verify it's a number, not a string
      expect(typeof lastCall[lastCall.length - 1]).toBe("number");
    });

    it("rejects non-numeric inputs when itemType is 'integer'", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={mockOnChange} itemType="integer" initialValues={[1, 2]} />);

      const input = screen.getByRole("combobox");
      await userEvent.type(input, "not-a-number{enter}");

      // Verify no onChange call was made with the invalid input
      expect(mockOnChange).not.toHaveBeenCalledWith(expect.arrayContaining(["not-a-number"]));
      // Check that only the initial values are visible
      expect(screen.getByText("1")).toBeInTheDocument();
      expect(screen.getByText("2")).toBeInTheDocument();
      expect(screen.queryByText("not-a-number")).not.toBeInTheDocument();
    });

    it("only uses the numeric prefix from mixed inputs for integer types", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={mockOnChange} itemType="integer" initialValues={[1, 2]} />);

      const input = screen.getByRole("combobox");
      await userEvent.type(input, "123abc{enter}");

      // The last call should have parsed only the numeric prefix
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];

      // Verify it parsed only the "123" part
      expect(lastCall).toContain(123);

      // Verify it didn't add the full "123abc" string
      expect(lastCall).not.toContainEqual("123abc");

      // Verify it's a number type, not a string
      expect(typeof lastCall[lastCall.length - 1]).toBe("number");

      // Check that the UI shows "123" but not "123abc"
      expect(screen.getByText("123")).toBeInTheDocument();
      expect(screen.queryByText("123abc")).not.toBeInTheDocument();
    });

    it("can delete the last character of a draft when itemType is 'integer'", async () => {
      const mockOnChange = jest.fn();
      render(<TagInputWithWrapper onChange={mockOnChange} itemType="integer" initialValues={[1, 2]} />);

      const input = screen.getByRole("combobox");

      // Type a number and verify it's shown in the onChange call
      await userEvent.type(input, "3");
      expect(mockOnChange).toHaveBeenCalled();

      // The current fieldValue should include the new draft value
      const withDraft = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];
      expect(withDraft).toEqual([1, 2, 3]);

      mockOnChange.mockClear();

      // Now delete the last character using backspace
      await userEvent.type(input, "{backspace}");

      // Verify that onChange was called to remove the draft value
      expect(mockOnChange).toHaveBeenCalled();

      // The final fieldValue should NOT include the draft value anymore
      const afterDelete = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1][0];
      expect(afterDelete).toEqual([1, 2]);

      // Verify the input field is cleared
      expect(input).toHaveValue("");
    });
  });
});
