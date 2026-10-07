import { fireEvent, render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useFusionUsage } from "core/api/cloud";
import { FusionUsageRead } from "core/api/types/AirbyteClient";
import messages from "locales/en.json";

import { AgentToolCallUsage } from "./AgentToolCallUsage";

jest.mock("core/api/cloud", () => ({ useFusionUsage: jest.fn() }));

const data: FusionUsageRead = {
  organizationId: "org",
  freeCapsApplicable: true,
  day: { used: 0, limit: 300, windowStart: "2026-10-02T00:00:00Z", resetsAt: "2026-10-03T00:00:00Z" },
  month: { used: 38_500, limit: 100_000, windowStart: "2026-10-01T00:00:00Z", resetsAt: "2026-11-01T00:00:00Z" },
};
const refetch = jest.fn();

type UsageState = Pick<
  ReturnType<typeof useFusionUsage>,
  "data" | "isEligible" | "isInitialLoading" | "isError" | "refetch"
>;

const renderCard = (overrides: Partial<UsageState> = {}) => {
  const query: UsageState = {
    data,
    isEligible: true,
    isInitialLoading: false,
    isError: false,
    refetch,
    ...overrides,
  };
  jest.mocked(useFusionUsage).mockReturnValue(query as ReturnType<typeof useFusionUsage>);
  return render(
    <IntlProvider locale="en" messages={messages}>
      <AgentToolCallUsage />
    </IntlProvider>
  );
};

describe("AgentToolCallUsage", () => {
  beforeEach(() => jest.clearAllMocks());

  it("renders monthly organization totals independently of daily usage", () => {
    renderCard();
    expect(screen.getByTestId("agent-tool-call-used")).toHaveTextContent("38,500");
    expect(screen.getByTestId("agent-tool-call-limit")).toHaveTextContent("100,000");
    expect(screen.getByTestId("agent-tool-call-percentage")).toHaveTextContent("38.5%");
    expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "38.5");
    expect(screen.getByRole("link", { name: "Learn more →" })).toHaveAttribute(
      "href",
      "https://docs.airbyte.com/platform/airbyte-mcp"
    );
  });

  it("renders zero usage", () => {
    renderCard({ data: { ...data, month: { ...data.month, used: 0 } } });
    expect(screen.getByTestId("agent-tool-call-used")).toHaveTextContent("0");
    expect(screen.getByTestId("agent-tool-call-percentage")).toHaveTextContent("0%");
  });

  it.each([100_000, 110_000])("renders the exhausted state at %s calls and caps the bar at 100%", (used) => {
    renderCard({ data: { ...data, month: { ...data.month, used } } });
    expect(screen.getByTestId("agent-tool-call-percentage")).toHaveTextContent(`${used / 1000}%`);
    const bar = screen.getByRole("progressbar");
    expect(bar).toHaveAttribute("aria-valuenow", "100");
    expect(bar.firstChild).toHaveStyle({ width: "100%" });
    expect(bar.firstChild).toHaveClass("usage__fill--exhausted");
  });

  it("does not turn unavailable usage into zero and offers retry", () => {
    renderCard({ data: { ...data, month: { ...data.month, used: null } } });
    expect(screen.getByTestId("agent-tool-call-used")).toHaveTextContent("Unavailable");
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("shows unlimited and untracked values without inventing a percentage", () => {
    renderCard({ data: { ...data, month: { ...data.month, used: null, limit: null } } });
    expect(screen.getByTestId("agent-tool-call-used")).toHaveTextContent("Not tracked");
    expect(screen.getByTestId("agent-tool-call-limit")).toHaveTextContent("Unlimited");
    expect(screen.getByTestId("agent-tool-call-percentage")).toHaveTextContent("N/A");
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });

  it("does not divide by a zero limit", () => {
    renderCard({ data: { ...data, month: { ...data.month, used: 0, limit: 0 } } });
    expect(screen.getByTestId("agent-tool-call-percentage")).toHaveTextContent("N/A");
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });

  it("does not present free allowances when they do not apply", () => {
    renderCard({ data: { ...data, freeCapsApplicable: false } });
    expect(screen.getByText("Free tool-call allowances do not apply to your organization.")).toBeInTheDocument();
    expect(screen.queryByTestId("agent-tool-call-used")).not.toBeInTheDocument();
    expect(screen.queryByText(/Tool calls are free/)).not.toBeInTheDocument();
  });

  it("renders inline failures and retries the query", () => {
    renderCard({ data: undefined, isError: true });
    expect(screen.getByText("Unable to load tool-call usage. Please try again.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("renders an accessible loading state without totals", () => {
    renderCard({ data: undefined, isInitialLoading: true });
    expect(screen.getByLabelText("Loading usage data...")).toHaveAttribute("aria-busy", "true");
    expect(screen.queryByTestId("agent-tool-call-used")).not.toBeInTheDocument();
  });

  it("hides the card for ineligible organizations even if cached data is present", () => {
    renderCard({ isEligible: false });
    expect(screen.queryByTestId("agent-tool-call-usage")).not.toBeInTheDocument();
  });
});
