import { AnalyticsService } from "./AnalyticsService";
import { Action, Namespace } from "./types";

describe("AnalyticsService", () => {
  beforeEach(() => {
    window.analytics = {
      track: jest.fn(),
      alias: jest.fn(),
      group: jest.fn(),
      identify: jest.fn(),
      page: jest.fn(),
      reset: jest.fn(),
      user: jest.fn(),
      setAnonymousId: jest.fn(),
      init: jest.fn(),
      use: jest.fn(),
      addIntegration: jest.fn(),
      load: jest.fn(),
      trackLink: jest.fn(),
      trackForm: jest.fn(),
      ready: jest.fn(),
      debug: jest.fn(),
      on: jest.fn(),
      timeout: jest.fn(),
    };
  });

  it("should send events to segment", () => {
    const service = new AnalyticsService();
    service.track(Namespace.CONNECTION, Action.CREATE, {});
    expect(window.analytics.track).toHaveBeenCalledWith("Airbyte.UI.Connection.Create", expect.anything());
  });

  it("should send version and environment for prod", () => {
    const service = new AnalyticsService();
    service.setContext({ airbyte_version: "0.42.13", environment: "prod" });
    service.track(Namespace.CONNECTION, Action.CREATE, {});
    expect(window.analytics.track).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ environment: "prod", airbyte_version: "0.42.13" })
    );
  });

  it("should send version and environment for dev", () => {
    const service = new AnalyticsService();
    service.setContext({ airbyte_version: "dev", environment: "dev" });
    service.track(Namespace.CONNECTION, Action.CREATE, {});
    expect(window.analytics.track).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ environment: "dev", airbyte_version: "dev" })
    );
  });

  it("should pass parameters to segment event", () => {
    const service = new AnalyticsService();
    service.track(Namespace.CONNECTION, Action.CREATE, { actionDescription: "Created new connection" });
    expect(window.analytics.track).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actionDescription: "Created new connection" })
    );
  });

  it("should pass context parameters to segment event", () => {
    const service = new AnalyticsService();
    service.setContext({ context: 42 });
    service.track(Namespace.CONNECTION, Action.CREATE, { actionDescription: "Created new connection" });
    expect(window.analytics.track).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ actionDescription: "Created new connection", context: 42 })
    );
  });

  describe("HockeyStack identify", () => {
    beforeEach(() => {
      jest.useFakeTimers();
      window.HockeyStack = undefined;
    });

    afterEach(() => {
      jest.useRealTimers();
      window.HockeyStack = undefined;
    });

    it("identifies immediately when HockeyStack is already loaded", () => {
      const identify = jest.fn();
      window.HockeyStack = { identify };
      const service = new AnalyticsService();
      service.identify("user-1", { email: "user@example.com" });
      expect(identify).toHaveBeenCalledWith("user@example.com", {
        email: "user@example.com",
        airbyte_user_id: "user-1",
      });
    });

    it("replays the latest identify once HockeyStack loads later", () => {
      const service = new AnalyticsService();
      service.identify("user-1", { email: "first@example.com" });
      service.identify("user-2", { email: "second@example.com" });

      const identify = jest.fn();
      window.HockeyStack = { identify };
      jest.advanceTimersByTime(2000);

      expect(identify).toHaveBeenCalledTimes(1);
      expect(identify).toHaveBeenCalledWith("second@example.com", {
        email: "second@example.com",
        airbyte_user_id: "user-2",
      });

      jest.advanceTimersByTime(10000);
      expect(identify).toHaveBeenCalledTimes(1);
    });

    it("replays identify when HockeyStack loads long after the identify call", () => {
      const service = new AnalyticsService();
      service.identify("user-1", { email: "user@example.com" });

      // Simulate the visitor granting consent 10 minutes after load.
      jest.advanceTimersByTime(10 * 60 * 1000);

      const identify = jest.fn();
      window.HockeyStack = { identify };
      jest.advanceTimersByTime(2000);

      expect(identify).toHaveBeenCalledTimes(1);
      expect(identify).toHaveBeenCalledWith("user@example.com", {
        email: "user@example.com",
        airbyte_user_id: "user-1",
      });
      expect(jest.getTimerCount()).toBe(0);
    });
  });
});
