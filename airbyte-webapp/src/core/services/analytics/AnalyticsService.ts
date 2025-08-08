import { PostHog } from "posthog-js";

import { HockeyStackAnalyticsObject } from "./HockeyStackAnalytics";
import { Action, EventParams, Namespace } from "./types";

type Context = Record<string, unknown>;

export class AnalyticsService {
  private context: Context = {};

  private getSegmentAnalytics = (): SegmentAnalytics.AnalyticsJS | undefined => window.analytics;

  private getHockeyStackAnalytics = (): HockeyStackAnalyticsObject | undefined => window.HockeyStack;

  private getPosthogAnalytics = (): PostHog | undefined => window.posthog;

  public setContext(context: Context) {
    this.context = {
      ...this.context,
      ...context,
    };
  }

  public hasContext(key: string): boolean {
    return this.context[key] !== undefined;
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

    const session_id = this.getPosthogAnalytics()?.get_session_id();

    this.getSegmentAnalytics()?.page?.(name, {
      ...params,
      ...this.context,
      ...(session_id && { $session_id: session_id }),
    });
  }

  public reset(): void {
    this.getSegmentAnalytics()?.reset?.();
  }

  public track(namespace: Namespace, action: Action, params: EventParams & { actionDescription?: string }) {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Track] Airbyte.UI.${namespace}.${action}`, "color: teal", params);
    }
    const session_id = this.getPosthogAnalytics()?.get_session_id();

    this.getSegmentAnalytics()?.track(`Airbyte.UI.${namespace}.${action}`, {
      ...params,
      ...this.context,
      ...(session_id && { $session_id: session_id }),
    });
  }

  public identify(userId: string, traits: Record<string, unknown> = {}): void {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Identify] ${userId}`, "color: teal", traits);
    }
    this.getSegmentAnalytics()?.identify?.(userId, traits);
    // PostHog identify and alias to link anonymous history
    const posthog = this.getPosthogAnalytics();
    if (posthog) {
      posthog.identify(userId, traits);
    }

    // HockeyStack supports string, boolean and number custom properties
    // https://docs.hockeystack.com/advanced-strategies-and-techniques/advanced-features/identifying-users
    const booleanNumberAndStringTraits = Object.entries(traits).reduce(
      (acc, [key, value]) => {
        if (typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
          acc[key] = value;
        }
        return acc;
      },
      {} as Record<string, string | number | boolean>
    );
    this.getHockeyStackAnalytics()?.identify?.(userId, booleanNumberAndStringTraits);
  }

  public group(organisationId: string, traits: Record<string, unknown> = {}): void {
    this.getSegmentAnalytics()?.group?.(organisationId, traits);
  }

  public setAnonymousId(anonymousId: string) {
    this.getSegmentAnalytics()?.setAnonymousId(anonymousId);
  }
}
