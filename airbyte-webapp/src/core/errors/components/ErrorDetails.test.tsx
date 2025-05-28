import { render } from "test-utils";

import { HttpProblem } from "core/api";

import { ErrorDetails } from "./ErrorDetails";
import { I18nError } from "../I18nError";

jest.mock("locales/en.json", () => ({
  ...jest.requireActual("locales/en.json"),
  "error.invalid": "This was invalid: {reason}",
}));

jest.mock("locales/en.errors.json", () => ({
  "dont-panic": "Don't panic!",
}));

// Mock the AirbyteTheme
jest.mock("hooks/theme/useAirbyteTheme", () => {
  const themeContextValue = {
    theme: "airbyteThemeLight",
    colorValues: {},
    setTheme: jest.fn(),
  };

  return {
    AirbyteThemeProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    useAirbyteTheme: jest.fn().mockReturnValue(themeContextValue),
    AirbyteThemeContext: {
      Provider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    },
  };
});

describe("ErrorDetails", () => {
  it("should render a standard error by its message", async () => {
    const result = await render(<ErrorDetails error={new Error("Test error")} />);
    expect(result.queryByText("Test error")).toBeInTheDocument();
  });

  it("should render an I18nError with the correct message", async () => {
    const result = await render(<ErrorDetails error={new I18nError("error.invalid", { reason: "42" })} />);
    expect(result.queryByText("This was invalid: 42")).toBeInTheDocument();
  });

  /* eslint-disable @typescript-eslint/no-explicit-any */
  it("should render a HttpProblem with documentation link", async () => {
    const result = await render(
      <ErrorDetails
        error={
          new HttpProblem({ method: "GET", url: "/some-url" }, 404, {
            type: "error:dont-panic" as any,
            title: "Don't panic" as any,
            documentationUrl: "https://airbyte.dev/dont-panic",
          })
        }
      />
    );
    expect(result.queryByText("Don't panic!")).toBeInTheDocument();
    expect(result.getByRole("link", { name: "Read more" })).toHaveProperty("href", "https://airbyte.dev/dont-panic");
  });
  /* eslint-enable @typescript-eslint/no-explicit-any */
});
