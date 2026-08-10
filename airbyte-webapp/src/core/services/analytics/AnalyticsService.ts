import { Action, EventParams, Namespace } from "./types";

declare global {
  interface Window {
    HockeyStack?: HockeyStackAnalyticsObject;
  }
}

export interface HockeyStackAnalyticsObject {
  // https://docs.hockeystack.com/technical-details/tracking/identifying-users
  identify: (identifier: string, customProperties?: Record<string, string | number | boolean>) => void;
}

type Context = Record<string, unknown>;

const HOCKEYSTACK_RETRY_INTERVAL_MS = 2000;

export class AnalyticsService {
  private context: Context = {};

  // HockeyStack is injected by GTM once the visitor consents, which can happen after
  // identify() fires. Keep the latest identify around and replay it when the script shows up.
  private pendingHockeyStackIdentify?: () => void;
  private hockeyStackRetryTimer?: ReturnType<typeof setInterval>;

  private getSegmentAnalytics = (): SegmentAnalytics.AnalyticsJS | undefined => window.analytics;

  private getHockeyStackAnalytics = (): HockeyStackAnalyticsObject | undefined => window.HockeyStack;

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

    this.getSegmentAnalytics()?.page?.(name, {
      ...params,
      ...this.context,
    });
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
    });
  }

  public identify(userId: string, traits: Record<string, unknown> = {}): void {
    if (process.env.NODE_ENV === "development") {
      console.debug(`%c[Analytics.Identify] ${userId}`, "color: teal", traits);
    }
    this.getSegmentAnalytics()?.identify?.(userId, traits);

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
    const email = booleanNumberAndStringTraits.email;
    if (typeof email === "string") {
      this.identifyHockeyStack(email, {
        ...booleanNumberAndStringTraits,
        airbyte_user_id: userId,
      });
    }
  }

  private identifyHockeyStack(email: string, properties: Record<string, string | number | boolean>): void {
    const hockeyStack = this.getHockeyStackAnalytics();
    if (hockeyStack?.identify) {
      hockeyStack.identify(email, properties);
      return;
    }

    this.pendingHockeyStackIdentify = () => this.getHockeyStackAnalytics()?.identify?.(email, properties);

    if (this.hockeyStackRetryTimer !== undefined) {
      return;
    }

    // Consent (and therefore the GTM-injected script) can arrive at any point in the page's
    // lifetime, so keep polling until the identify lands.
    this.hockeyStackRetryTimer = setInterval(() => {
      if (this.getHockeyStackAnalytics()?.identify) {
        this.pendingHockeyStackIdentify?.();
        clearInterval(this.hockeyStackRetryTimer);
        this.hockeyStackRetryTimer = undefined;
        this.pendingHockeyStackIdentify = undefined;
      }
    }, HOCKEYSTACK_RETRY_INTERVAL_MS);
  }

  public group(organisationId: string, traits: Record<string, unknown> = {}): void {
    this.getSegmentAnalytics()?.group?.(organisationId, traits);
  }

  public setAnonymousId(anonymousId: string) {
    this.getSegmentAnalytics()?.setAnonymousId(anonymousId);
  }
}
