import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { CopyButton } from "./CopyButton";

const CONTENT = "copy-me";

const setClipboard = (value: unknown) => {
  Object.defineProperty(navigator, "clipboard", { value, writable: true, configurable: true });
};

const clickCopy = async () => {
  await userEvent.click(screen.getByTestId("copy-button"));
};

describe("CopyButton", () => {
  let consoleError: jest.SpyInstance;

  beforeEach(() => {
    setClipboard({ writeText: jest.fn().mockResolvedValue(undefined) });
    consoleError = jest.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    consoleError.mockRestore();
  });

  it("writes the content and calls onCopy on a successful copy", async () => {
    const onCopy = jest.fn();
    const { container } = await render(<CopyButton content={CONTENT} onCopy={onCopy} />);

    await clickCopy();

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(CONTENT);
    await waitFor(() => expect(onCopy).toHaveBeenCalledTimes(1));
    expect(container.querySelector('[data-icon="success-filled"]')).toBeInTheDocument();
  });

  it("resolves a function `content` at click time", async () => {
    await render(<CopyButton content={() => CONTENT} />);

    await clickCopy();

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(CONTENT);
  });

  it("calls onCopyError when the clipboard write is rejected", async () => {
    setClipboard({ writeText: jest.fn().mockRejectedValue(new Error("clipboard blocked")) });
    const onCopy = jest.fn();
    const onCopyError = jest.fn();
    const { container } = await render(<CopyButton content={CONTENT} onCopy={onCopy} onCopyError={onCopyError} />);

    await clickCopy();

    await waitFor(() => expect(onCopyError).toHaveBeenCalledTimes(1));
    expect(onCopy).not.toHaveBeenCalled();
    expect(container.querySelector('[data-icon="success-filled"]')).not.toBeInTheDocument();
  });

  it("calls onCopyError when the clipboard API is unavailable", async () => {
    // Outside a secure context `navigator.clipboard` is undefined entirely, so reading `.writeText`
    // off it throws synchronously - there is never a promise for `.catch()` to see.
    setClipboard(undefined);
    const onCopyError = jest.fn();
    await render(<CopyButton content={CONTENT} onCopyError={onCopyError} />);

    await clickCopy();

    expect(onCopyError).toHaveBeenCalledTimes(1);
  });

  it("logs a rejected write when the consumer passes no onCopyError", async () => {
    // Handling the rejection stops it reaching the browser as an unhandled rejection, so without
    // this log the failure would be invisible to the ~all consumers that pass no onCopyError.
    setClipboard({ writeText: jest.fn().mockRejectedValue(new Error("clipboard blocked")) });
    await render(<CopyButton content={CONTENT} />);

    await clickCopy();

    await waitFor(() => expect(consoleError).toHaveBeenCalledTimes(1));
  });

  it("logs an unavailable clipboard when the consumer passes no onCopyError", async () => {
    setClipboard(undefined);
    await render(<CopyButton content={CONTENT} />);

    await clickCopy();

    expect(consoleError).toHaveBeenCalledTimes(1);
  });

  it("falls back to a generic accessible name and lets `title` override it", async () => {
    const { unmount } = await render(<CopyButton content={CONTENT} />);
    expect(screen.getByRole("button", { name: "Copy" })).toBeInTheDocument();
    unmount();

    await render(<CopyButton content={CONTENT} title="Copy bearer token" />);
    expect(screen.getByRole("button", { name: "Copy bearer token" })).toBeInTheDocument();
  });
});
