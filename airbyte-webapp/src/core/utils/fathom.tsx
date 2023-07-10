declare global {
  interface Window {
    fathom?: FathomAnalytics;
  }
}

export interface FathomAnalytics {
  trackPageview: (opts?: PageViewOptions) => void;
  trackGoal: (code: string, cents: number) => void;
}
export interface PageViewOptions {
  url?: string;
  referrer?: string;
}

const FATHOM_EVENTS = {
  "Airbyte.UI.User.Create": { id: "QS4WUT2A", value: 2400 },
};

export type FathomEvent = keyof typeof FATHOM_EVENTS;
export const loadFathom = (token?: string): void => {
  if (token && typeof window !== "undefined" && !window.fathom) {
    const script = document.createElement("script");
    script.src = "https://mohtaf.airbyte.com/script.js";
    script.defer = true;
    script.setAttribute("data-site", token);
    script.setAttribute("data-auto", "false");

    document.head.appendChild(script);
  }
};

export const trackPageview = (): void => window.fathom?.trackPageview();
const trackGoal = (eventId: string, cents: number): void => window.fathom?.trackGoal(eventId, cents);

export const trackSignup = (isCorporate: boolean): void => {
  const event = FATHOM_EVENTS["Airbyte.UI.User.Create"];
  trackGoal(event.id, isCorporate ? event.value : 0);
};
