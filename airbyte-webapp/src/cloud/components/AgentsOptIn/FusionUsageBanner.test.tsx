import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useFusionUsage } from "core/api/cloud";
import { FusionUsageRead } from "core/api/types/AirbyteClient";
import messages from "locales/en.json";

import { FusionUsageBanner } from "./FusionUsageBanner";

jest.mock("core/api/cloud", () => ({ useFusionUsage: jest.fn() }));

const data: FusionUsageRead = {
  organizationId: "org",
  freeCapsApplicable: true,
  day: { used: 300, limit: 300, windowStart: "2026-10-02T00:00:00Z", resetsAt: "2026-10-03T00:00:00Z" },
  month: { used: 100_000, limit: 100_000, windowStart: "2026-10-01T00:00:00Z", resetsAt: "2026-11-01T00:00:00Z" },
};

const renderBanner = (overrides: object = {}) => {
  jest
    .mocked(useFusionUsage)
    .mockReturnValue({ data, isEligible: true, isError: false, ...overrides } as ReturnType<typeof useFusionUsage>);
  return render(
    <IntlProvider locale="en" messages={messages} timeZone="America/Los_Angeles">
      <FusionUsageBanner />
    </IntlProvider>
  );
};

describe("FusionUsageBanner", () => {
  beforeEach(() => jest.spyOn(Date, "now").mockReturnValue(Date.parse("2026-10-02T00:00:00Z")));
  afterEach(() => jest.restoreAllMocks());

  it.each([100_000, 110_000])("warns at %s calls and formats reset in UTC rather than the user's timezone", (used) => {
    renderBanner({ data: { ...data, month: { ...data.month, used } } });
    expect(screen.getByTestId("fusion-usage-banner")).toHaveTextContent(
      "Your organization has used all of its agent tool calls this month. Your limit resets on November 1."
    );
  });

  it.each([
    ["daily cap only", { ...data.month, used: 30 }],
    ["unknown count", { ...data.month, used: null }],
    ["unlimited", { ...data.month, limit: null }],
    ["zero limit", { ...data.month, limit: 0 }],
    ["expired period", { ...data.month, resetsAt: "2026-10-01T00:00:00Z" }],
  ])("does not warn for %s", (_name, month) => {
    renderBanner({ data: { ...data, month } });
    expect(screen.queryByTestId("fusion-usage-banner")).not.toBeInTheDocument();
  });

  it.each([
    ["ineligible organization", { isEligible: false }],
    ["failed refresh", { isError: true }],
    ["inapplicable allowances", { data: { ...data, freeCapsApplicable: false } }],
    ["no data", { data: undefined }],
  ])("does not warn with %s", (_name, overrides) => {
    renderBanner(overrides);
    expect(screen.queryByTestId("fusion-usage-banner")).not.toBeInTheDocument();
  });
});
