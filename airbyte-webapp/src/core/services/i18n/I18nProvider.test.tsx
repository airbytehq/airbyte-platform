import { render, renderHook, act } from "@testing-library/react";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { I18nProvider, useI18nContext } from "./I18nProvider";

jest.mock("locales/en.json", () => ({
  test: "default message",
  "test.id": "Hello world!",
  "test.id.bold": "Hello <b>world</b>!",
}));

const provider = (locale = "en"): React.FC<React.PropsWithChildren<unknown>> => {
  return ({ children }) => <I18nProvider locale={locale}>{children}</I18nProvider>;
};

const useMessages = () => {
  const { setMessageOverwrite } = useI18nContext();
  const { messages } = useIntl();
  return { setMessageOverwrite, messages };
};

describe("I18nProvider", () => {
  it("should set the react-intl locale correctly", () => {
    const { result } = renderHook(() => useIntl(), {
      wrapper: provider("fr"),
    });
    expect(result.current.locale).toBe("fr");
  });

  it("should set messages for consumption via react-intl", () => {
    const wrapper = render(
      <span data-testid="msg">
        <FormattedMessage id="test.id" />
      </span>,
      { wrapper: provider() }
    );
    expect(wrapper.getByTestId("msg").textContent).toBe("Hello world!");
  });

  it("should pick the browser locale if no locale is specified", () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    jest.spyOn(Intl.DateTimeFormat.prototype, "resolvedOptions").mockReturnValue({ locale: "de-DE" } as any);
    const { result } = renderHook(() => useIntl(), {
      wrapper: ({ children }) => <I18nProvider>{children}</I18nProvider>,
    });
    expect(result.current.locale).toBe("de-DE");
  });

  it("should use the browser locale for formatting if no locale is specified", () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    jest.spyOn(Intl.DateTimeFormat.prototype, "resolvedOptions").mockReturnValue({ locale: "de-DE" } as any);
    const { result } = renderHook(() => useIntl(), {
      wrapper: ({ children }) => <I18nProvider>{children}</I18nProvider>,
    });
    expect(result.current.formatNumber(1_000_000.42)).toBe("1.000.000,42");
  });

  it("should allow render <b></b> tags for every message", () => {
    const wrapper = render(
      <span data-testid="msg">
        <FormattedMessage id="test.id.bold" />
      </span>,
      { wrapper: provider() }
    );
    expect(wrapper.getByTestId("msg").innerHTML).toBe("Hello <strong>world</strong>!");
  });

  describe("useI18nContext", () => {
    it("should allow overwriting default and setting additional messages", () => {
      const { result } = renderHook(() => useMessages(), {
        wrapper: provider(),
      });
      expect(result.current.messages).toHaveProperty("test", "default message");
      act(() => result.current.setMessageOverwrite({ test: "overwritten message", other: "new message" }));
      expect(result.current.messages).toHaveProperty("test", "overwritten message");
      expect(result.current.messages).toHaveProperty("other", "new message");
    });

    it("should allow resetting overwrites with an empty object", () => {
      const { result } = renderHook(() => useMessages(), {
        wrapper: provider(),
      });
      act(() => result.current.setMessageOverwrite({ test: "overwritten message" }));
      expect(result.current.messages).toHaveProperty("test", "overwritten message");
      act(() => result.current.setMessageOverwrite({}));
      expect(result.current.messages).toHaveProperty("test", "default message");
    });

    it("should allow resetting overwrites with undefined", () => {
      const { result } = renderHook(() => useMessages(), {
        wrapper: provider(),
      });
      act(() => result.current.setMessageOverwrite({ test: "overwritten message" }));
      expect(result.current.messages).toHaveProperty("test", "overwritten message");
      act(() => result.current.setMessageOverwrite(undefined));
      expect(result.current.messages).toHaveProperty("test", "default message");
    });
  });
});
