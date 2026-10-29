import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils";

import { CloudBannerGroup } from "./CloudBannerGroup";

interface MockBanner {
  id: string;
  kind: "hideable" | "warning" | "error";
}
const STORAGE_KEY = "airbyte_banner-group-state";

let mockUserId = "user-1";
let mockBanners: MockBanner[] = [];

jest.mock("core/services/auth", () => ({
  useCurrentUser: () => ({ userId: mockUserId }),
}));

// Each stub renders an empty marker, so tests can check where the real banners sit in the group.
jest.mock("cloud/area/billing/components/StatusBanner", () => ({
  StatusBanner: () => <span data-testid="slot-status" />,
}));
jest.mock("cloud/components/AdpOrganizationBanner", () => ({
  AdpOrganizationBanner: () => <span data-testid="slot-adp" />,
}));
jest.mock("cloud/components/AgentsOptIn", () => ({
  AgentsOptInBanner: () => <span data-testid="slot-agents" />,
}));

jest.mock("cloud/components/AgentsOptIn/FusionUsageBanner", () => {
  const MockBanners = () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { HideableBanner } = require("./components/HideableBanner");
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { AlertBanner } = require("components/ui/Banner/AlertBanner");

    return (
      <>
        <span data-testid="slot-fusion" />
        {mockBanners.map(({ id, kind }) =>
          kind === "hideable" ? (
            <HideableBanner key={id} id={id} message={`message ${id}`} />
          ) : (
            <AlertBanner key={id} color={kind} message={`message ${id}`} />
          )
        )}
      </>
    );
  };

  return { FusionUsageBanner: MockBanners };
});

const persist = (state: Record<string, { isCollapsed: boolean; seenIds: string[] }>) =>
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
const persistCollapsed = (...seenIds: string[]) => persist({ "user-1": { isCollapsed: true, seenIds } });

const hideable = (...ids: string[]): MockBanner[] => ids.map((id) => ({ id, kind: "hideable" }));
const warning = (id: string): MockBanner => ({ id, kind: "warning" });
const error = (id: string): MockBanner => ({ id, kind: "error" });

const visibleBanners = () => screen.queryAllByText(/^message /).map((banner) => banner.textContent);
const expandedToggle = () => screen.getByTestId("cloud-banner-group-expanded");
const collapsedToggle = () => screen.getByTestId("cloud-banner-group-collapsed");

const group = () => (
  <TestWrapper>
    <CloudBannerGroup />
  </TestWrapper>
);

// When the group collapses, expands and counts is decided by useBannerGroupState and tested there.
// These tests cover what the group itself does with that state.
describe(`${CloudBannerGroup.name}`, () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockUserId = "user-1";
    mockBanners = [];
  });

  describe("rendering", () => {
    it("should render every banner when nothing is persisted", () => {
      mockBanners = [...hideable("a"), warning("w"), ...hideable("c")];
      render(group());

      expect(visibleBanners()).toEqual(["message a", "message w", "message c"]);
    });

    it("should place the banners that can be warnings before the ones that are always info", () => {
      render(group());

      const slots = Array.from(screen.getByTestId("cloud-banner-group").children)
        .map((child) => child.getAttribute("data-testid"))
        .filter((testId) => testId?.startsWith("slot-"));
      expect(slots).toEqual(["slot-fusion", "slot-status", "slot-agents", "slot-adp"]);
    });

    it.each([
      ["collapsed", true],
      ["expanded", false],
    ])("should render the toggle after every banner when %s", (_name, isCollapsed) => {
      persist({ "user-1": { isCollapsed, seenIds: ["a", "b"] } });
      mockBanners = hideable("a", "b");
      render(group());

      const children = Array.from(screen.getByTestId("cloud-banner-group").children);
      expect(children[children.length - 1]).toBe(screen.getByRole("button"));
    });
  });

  describe("toggle visibility", () => {
    it("should show no toggle without hideable banners", () => {
      render(group());

      expect(screen.queryByRole("button")).not.toBeInTheDocument();
    });

    it("should show no toggle when there are only warning and error banners", () => {
      mockBanners = [warning("w"), error("e")];
      render(group());

      expect(screen.queryByRole("button")).not.toBeInTheDocument();
    });

    it("should show the expanded toggle with two hideable banners", () => {
      mockBanners = hideable("a", "b");
      render(group());

      expect(expandedToggle()).toHaveAttribute("aria-expanded", "true");
    });
  });

  describe("when collapsed is persisted and every banner has been seen", () => {
    beforeEach(() => {
      persistCollapsed("a", "b");
      mockBanners = hideable("a", "b");
      render(group());
    });

    it("should hide the hideable banners", () => {
      expect(visibleBanners()).toEqual([]);
    });

    it("should show the collapsed toggle", () => {
      expect(collapsedToggle()).toHaveAttribute("aria-expanded", "false");
    });
  });

  describe("when the expanded toggle is clicked", () => {
    beforeEach(async () => {
      mockBanners = hideable("a", "b");
      render(group());
      await userEvent.click(expandedToggle());
    });

    it("should hide the hideable banners", () => {
      expect(visibleBanners()).toEqual([]);
    });

    it("should show the collapsed toggle", () => {
      expect(collapsedToggle()).toBeVisible();
    });
  });

  describe("when the collapsed toggle is clicked", () => {
    beforeEach(async () => {
      persistCollapsed("a", "b");
      mockBanners = hideable("a", "b");
      render(group());
      await userEvent.click(collapsedToggle());
    });

    it("should show the hidden banners", () => {
      expect(visibleBanners()).toEqual(["message a", "message b"]);
    });

    it("should show the expanded toggle", () => {
      expect(expandedToggle()).toBeVisible();
    });
  });

  describe("when collapsed with warning and error banners", () => {
    beforeEach(() => {
      persistCollapsed("a", "b");
      mockBanners = [...hideable("a", "b"), warning("w"), error("e")];
      render(group());
    });

    it("should hide only the hideable banners", () => {
      expect(visibleBanners()).toEqual(["message w", "message e"]);
    });

    it("should count only the hideable banners in the toggle", () => {
      expect(collapsedToggle()).toHaveTextContent("2");
    });
  });
});
