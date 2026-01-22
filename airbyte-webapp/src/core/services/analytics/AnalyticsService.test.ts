import { PostHog } from "posthog-js";

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

  describe("Session ID tracking", () => {
    it("should include PostHog session_id in direct PostHog capture when sendToPosthog is true", () => {
      window.posthog = {
        get_session_id: jest.fn().mockReturnValue("test-session-789"),
        capture: jest.fn(),
        identify: jest.fn(),
      } as unknown as PostHog;

      const service = new AnalyticsService();
      service.track(Namespace.SOURCE, Action.CREATE, { source_name: "test-source" }, { sendToPosthog: true });

      // Verify PostHog receives the session ID
      expect(window.posthog.capture).toHaveBeenCalledWith(
        "Airbyte.UI.Source.Create",
        expect.objectContaining({
          source_name: "test-source",
          $session_id: "test-session-789",
        })
      );
    });

    it("should not include $session_id in PostHog capture when session is unavailable", () => {
      window.posthog = {
        get_session_id: jest.fn().mockReturnValue(undefined),
        capture: jest.fn(),
        identify: jest.fn(),
      } as unknown as PostHog;

      const service = new AnalyticsService();
      service.track(Namespace.SOURCE, Action.CREATE, { source_name: "test-source" }, { sendToPosthog: true });

      const captureCall = (window.posthog.capture as jest.Mock).mock.calls[0][1];
      expect(captureCall).not.toHaveProperty("$session_id");
    });

    it("should not error when PostHog is not initialized", () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).posthog = undefined;

      const service = new AnalyticsService();

      expect(() => {
        service.track(Namespace.SOURCE, Action.CREATE, { source_name: "test-source" }, { sendToPosthog: true });
      }).not.toThrow();
    });
  });
});
