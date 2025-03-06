import { Listbox, ListboxOptions, ListboxOption, ListboxButton } from "@headlessui/react";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { DATA_HEADLESS_UI_STATE_ATTRIBUTE, useHeadlessUiOnClose } from "./useHeadlessUiOnClose";

const LISTBOX_TEST_ID = "listbox-test";
const BUTTON_TEXT = "Options";

describe(`${useHeadlessUiOnClose.name}()`, () => {
  it("Listbox renders expected data attributes on open and close", async () => {
    await render(<MockListbox />);

    const listBox = screen.queryByTestId(LISTBOX_TEST_ID);
    const listBoxButton = screen.queryByText(BUTTON_TEXT);

    if (!listBox || !listBoxButton) {
      throw new Error("Listbox or ListboxButton not found");
    }

    // Open the listbox
    await userEvent.click(listBoxButton);

    // Check that the listbox has the expected data attribute
    expect(listBox).toHaveAttribute(DATA_HEADLESS_UI_STATE_ATTRIBUTE);
    expect(listBox?.getAttribute(DATA_HEADLESS_UI_STATE_ATTRIBUTE)).toEqual("open");

    // Close the listbox
    await userEvent.click(listBoxButton);

    // Check that the listbox has the expected data attribute
    expect(listBox).toHaveAttribute(DATA_HEADLESS_UI_STATE_ATTRIBUTE);
    expect(listBox?.getAttribute(DATA_HEADLESS_UI_STATE_ATTRIBUTE)).toEqual("");
  });

  it("Listbox calls onCloseCallback when closed", async () => {
    const onCloseCallback = jest.fn();
    await render(<MockListbox onCloseCallback={onCloseCallback} />);

    const listBoxButton = screen.queryByText(BUTTON_TEXT);

    if (!listBoxButton) {
      throw new Error("ListboxButton not found");
    }

    // Open the listbox
    await userEvent.click(listBoxButton);

    // Close the listbox
    await userEvent.click(listBoxButton);

    // Check that the onCloseCallback was called
    expect(onCloseCallback).toHaveBeenCalledTimes(1);
  });
});

const MockListbox = ({ onCloseCallback }: { onCloseCallback?: () => void }) => {
  const { targetRef } = useHeadlessUiOnClose(onCloseCallback);

  return (
    <Listbox as="div" data-testid={LISTBOX_TEST_ID} ref={targetRef}>
      <ListboxButton>{BUTTON_TEXT}</ListboxButton>
      <ListboxOptions>
        <ListboxOption value="1">Option 1</ListboxOption>
        <ListboxOption value="2">Option 2</ListboxOption>
      </ListboxOptions>
    </Listbox>
  );
};
