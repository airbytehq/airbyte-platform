import { isConsentManagerActive, isGdprCountry, loadConsentManager, showConsentPreferences } from "./dataPrivacy";

const mockTimeZone = (timeZone: string) => {
  jest.spyOn(Intl, "DateTimeFormat").mockImplementation(
    () =>
      ({
        resolvedOptions: () =>
          ({
            timeZone,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          }) as any,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      }) as any
  );
};

describe("dataPrivacy", () => {
  describe("isGdprCountry()", () => {
    afterEach(() => {
      jest.clearAllMocks();
    });

    it("should return true for timezones inside EU", () => {
      mockTimeZone("Europe/Berlin");
      expect(isGdprCountry()).toBe(true);
    });

    it("should return false for non EU countries", () => {
      mockTimeZone("America/Chicago");
      expect(isGdprCountry()).toBe(false);
    });
  });

  describe("loadConsentManager()", () => {
    const loaderSources = () => Array.from(document.head.querySelectorAll("script")).map((script) => script.src);

    beforeEach(() => {
      process.env.REACT_APP_DATAGRAIL_CUSTOMER_ID = "customer-id";
      process.env.REACT_APP_DATAGRAIL_CONTAINER_ID = "container-id";
    });

    afterEach(() => {
      delete process.env.REACT_APP_DATAGRAIL_CUSTOMER_ID;
      delete process.env.REACT_APP_DATAGRAIL_CONTAINER_ID;
      document.head.querySelectorAll("script").forEach((script) => script.remove());
      window.history.pushState({}, "", "/");
    });

    it("should load the DataGrail container", () => {
      loadConsentManager();

      expect(loaderSources()).toEqual([
        "https://api.consentjs.datagrail.io/customer-id/container-id/consent-loader.js",
      ]);
    });

    it("should not load anything when the container is not configured", () => {
      delete process.env.REACT_APP_DATAGRAIL_CONTAINER_ID;

      loadConsentManager();

      expect(loaderSources()).toEqual([]);
    });

    it("should not load anything in the embedded widget", () => {
      window.history.pushState({}, "", "/embedded-widget");

      loadConsentManager();

      expect(loaderSources()).toEqual([]);
    });
  });

  describe("isConsentManagerActive()", () => {
    afterEach(() => {
      delete window.DG_BANNER_API;
    });

    it("should return false before the DataGrail banner has loaded", () => {
      expect(isConsentManagerActive()).toBe(false);
    });

    it("should return true once the DataGrail banner has loaded", () => {
      window.DG_BANNER_API = {};
      expect(isConsentManagerActive()).toBe(true);
    });
  });

  describe("showConsentPreferences()", () => {
    afterEach(() => {
      delete window.DG_BANNER_API;
    });

    it("should open the categories layer when available", () => {
      const showLayer = jest.fn();
      const showConsentBanner = jest.fn();
      window.DG_BANNER_API = { showLayer, showConsentBanner };

      showConsentPreferences();

      expect(showLayer).toHaveBeenCalledWith("categories-layer");
      expect(showConsentBanner).not.toHaveBeenCalled();
    });

    it("should fall back to the consent banner when the categories layer is unavailable", () => {
      const showConsentBanner = jest.fn();
      window.DG_BANNER_API = { showConsentBanner };

      showConsentPreferences();

      expect(showConsentBanner).toHaveBeenCalled();
    });

    it("should not throw when the DataGrail banner has not loaded", () => {
      expect(() => showConsentPreferences()).not.toThrow();
    });
  });
});
