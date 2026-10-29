import { render, screen } from "@testing-library/react";

import { HideableBanner } from "./HideableBanner";
import { BannerGroupContext, BannerGroupContextValue } from "../../BannerGroupContext";

const createContextValue = (overrides: Partial<BannerGroupContextValue> = {}): BannerGroupContextValue => ({
  registerBanner: jest.fn(),
  unregisterBanner: jest.fn(),
  checkIsBannerHidden: () => false,
  ...overrides,
});

const renderInGroup = (value: BannerGroupContextValue, banner: React.ReactElement) =>
  render(<BannerGroupContext.Provider value={value}>{banner}</BannerGroupContext.Provider>);

describe(`${HideableBanner.name}`, () => {
  it("should render without a provider", () => {
    render(<HideableBanner id="test" message="Hello" data-testid="banner" />);

    expect(screen.getByTestId("banner")).toBeVisible();
  });

  it("should render the message it is given", () => {
    render(<HideableBanner id="test" message="Hello there" />);

    expect(screen.getByText("Hello there")).toBeVisible();
  });

  describe("registration", () => {
    it("should register on mount", () => {
      const value = createContextValue();

      renderInGroup(value, <HideableBanner id="test" message="Hello" />);

      expect(value.registerBanner).toHaveBeenCalledWith("test");
    });

    it("should not unregister while it is mounted", () => {
      const value = createContextValue();

      renderInGroup(value, <HideableBanner id="test" message="Hello" />);

      expect(value.unregisterBanner).not.toHaveBeenCalled();
    });

    it("should unregister on unmount", () => {
      const value = createContextValue();
      const { unmount } = renderInGroup(value, <HideableBanner id="test" message="Hello" />);

      unmount();

      expect(value.unregisterBanner).toHaveBeenCalledWith("test");
    });
  });

  describe("when the context hides it", () => {
    const hideFirst = createContextValue({ checkIsBannerHidden: (id) => id === "hidden" });

    beforeEach(() => {
      renderInGroup(
        hideFirst,
        <>
          <HideableBanner id="hidden" message="Hidden" data-testid="hidden-banner" />
          <HideableBanner id="shown" message="Shown" data-testid="shown-banner" />
        </>
      );
    });

    it("should not render the banner the context hides", () => {
      expect(screen.queryByTestId("hidden-banner")).not.toBeInTheDocument();
    });

    it("should still render the banner the context shows", () => {
      expect(screen.getByTestId("shown-banner")).toBeVisible();
    });

    it("should stay registered while hidden", () => {
      expect(hideFirst.registerBanner).toHaveBeenCalledWith("hidden");
    });
  });

  describe("when the context stops hiding it", () => {
    it("should render the banner again", () => {
      const banner = <HideableBanner id="test" message="Hello" data-testid="banner" />;
      const { rerender } = renderInGroup(createContextValue({ checkIsBannerHidden: () => true }), banner);

      rerender(
        <BannerGroupContext.Provider value={createContextValue({ checkIsBannerHidden: () => false })}>
          {banner}
        </BannerGroupContext.Provider>
      );

      expect(screen.getByTestId("banner")).toBeVisible();
    });
  });

  describe("when the id changes", () => {
    const value = createContextValue();

    beforeEach(() => {
      jest.clearAllMocks();
      const { rerender } = renderInGroup(value, <HideableBanner id="first" message="Hello" />);

      rerender(
        <BannerGroupContext.Provider value={value}>
          <HideableBanner id="second" message="Hello" />
        </BannerGroupContext.Provider>
      );
    });

    it("should register the first id first", () => {
      expect(value.registerBanner).toHaveBeenNthCalledWith(1, "first");
    });

    it("should unregister the first id", () => {
      expect(value.unregisterBanner).toHaveBeenCalledWith("first");
    });

    it("should register the second id", () => {
      expect(value.registerBanner).toHaveBeenNthCalledWith(2, "second");
    });
  });

  describe("when it rerenders with stable context callbacks", () => {
    const value = createContextValue();

    beforeEach(() => {
      jest.clearAllMocks();
      const { rerender } = renderInGroup(value, <HideableBanner id="test" message="Hello" />);

      rerender(
        <BannerGroupContext.Provider value={value}>
          <HideableBanner id="test" message="Changed text" />
        </BannerGroupContext.Provider>
      );
    });

    it("should register only once, not on every render", () => {
      expect(value.registerBanner).toHaveBeenCalledTimes(1);
    });

    it("should not unregister", () => {
      expect(value.unregisterBanner).not.toHaveBeenCalled();
    });
  });
});
