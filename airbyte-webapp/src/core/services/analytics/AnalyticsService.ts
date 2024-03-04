import { config } from "core/config";

import { Action, EventParams, Namespace } from "./types";

type Context = Record<string, unknown>;

export class AnalyticsService {
  private context: Context = {};

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

  public alias(newId: string): void {
    this.getSegmentAnalytics()?.alias?.(newId);
  }

  public page(name: string, params: EventParams = {}): void {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Page] ${name}`, "color: teal", params);
    }
    this.getSegmentAnalytics()?.page?.(name, { ...params, ...this.context });
  }

  public reset(): void {
    this.getSegmentAnalytics()?.reset?.();
  }

  public track(namespace: Namespace, action: Action, params: EventParams & { actionDescription?: string }) {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Track] Airbyte.UI.${namespace}.${action}`, "color: teal", params);
    }
    this.getSegmentAnalytics()?.track(`Airbyte.UI.${namespace}.${action}`, {
      ...params,
      ...this.context,
      airbyte_version: config.version,
      environment: config.version === "dev" ? "dev" : "prod",
    });
  }

  public identify(userId: string, traits: Record<string, unknown> = {}): void {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Identify] ${userId}`, "color: teal", traits);
    }
    this.getSegmentAnalytics()?.identify?.(userId, traits);
  }

  public group(organisationId: string, traits: Record<string, unknown> = {}): void {
    this.getSegmentAnalytics()?.group?.(organisationId, traits);
  }

  public setAnonymousId(anonymousId: string) {
    this.getSegmentAnalytics()?.setAnonymousId(anonymousId);
  }
}
