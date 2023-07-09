import { Action, EventParams, Namespace } from "./types";

type Context = Record<string, unknown>;

export class AnalyticsService {
  private context: Context = {};

  constructor(private version?: string) {}

  private getSegmentAnalytics = (): SegmentAnalytics.AnalyticsJS | undefined => window.analytics;

  public setContext(context: Context) {
    this.context = {
      ...this.context,
      ...context,
    };
  }

  public removeFromContext(...keys: string[]) {
    keys.forEach((key) => delete this.context[key]);
  }

  alias = (newId: string): void => this.getSegmentAnalytics()?.alias?.(newId);

  page = (name: string): void => {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Page] ${name}`, "color: teal");
    }
    this.getSegmentAnalytics()?.page?.(name, { ...this.context });
  };

  reset = (): void => this.getSegmentAnalytics()?.reset?.();

  track = (namespace: Namespace, action: Action, params: EventParams & { actionDescription?: string }) => {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Track] Airbyte.UI.${namespace}.${action}`, "color: teal", params);
    }
    this.getSegmentAnalytics()?.track(`Airbyte.UI.${namespace}.${action}`, {
      ...params,
      ...this.context,
      airbyte_version: this.version,
      environment: this.version === "dev" ? "dev" : "prod",
    });
  };

  identify = (userId: string, traits: Record<string, unknown> = {}): void => {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Identify] ${userId}`, "color: teal", traits);
    }
    this.getSegmentAnalytics()?.identify?.(userId, traits);
  };

  group = (organisationId: string, traits: Record<string, unknown> = {}): void =>
    this.getSegmentAnalytics()?.group?.(organisationId, traits);
  setAnonymousId = (anonymousId: string) => this.getSegmentAnalytics()?.setAnonymousId(anonymousId);
}
