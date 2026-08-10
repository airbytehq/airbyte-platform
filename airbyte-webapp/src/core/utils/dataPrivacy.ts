declare global {
  interface Window {
    DG_BANNER_API?: {
      showLayer?: (bannerApiId: string) => void;
      showConsentBanner?: () => void;
    };
  }
}

/**
 * Layer of the DataGrail banner layout that exposes the per-category toggles.
 * @see https://docs.datagrail.io/docs/consent/banner/layouts-overview/
 */
const DATAGRAIL_PREFERENCE_LAYER_ID = "categories-layer";

const GDPR_TIMEZONES = [
  "Africa/Ceuta",
  "Asia/Famagusta",
  "Asia/Nicosia",
  "Atlantic/Azores",
  "Atlantic/Canary",
  "Atlantic/Madeira",
  "Europe/Amsterdam",
  "Europe/Athens",
  "Europe/Berlin",
  "Europe/Bratislava",
  "Europe/Brussels",
  "Europe/Bucharest",
  "Europe/Budapest",
  "Europe/Busingen",
  "Europe/Copenhagen",
  "Europe/Dublin",
  "Europe/Helsinki",
  "Europe/Lisbon",
  "Europe/Ljubljana",
  "Europe/Luxembourg",
  "Europe/Madrid",
  "Europe/Malta",
  "Europe/Paris",
  "Europe/Prague",
  "Europe/Riga",
  "Europe/Rome",
  "Europe/Sofia",
  "Europe/Stockholm",
  "Europe/Tallinn",
  "Europe/Vienna",
  "Europe/Vilnius",
  "Europe/Warsaw",
  "Europe/Zagreb",
];

export const isGdprCountry = (): boolean => {
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return GDPR_TIMEZONES.includes(timeZone);
};

/**
 * Loads DataGrail, which injects the consent banner itself.
 *
 * The embedded widget renders inside a customer's own page, where their consent manager
 * applies, so it gets no banner of ours.
 */
export const loadConsentManager = (): void => {
  const customerId = process.env.REACT_APP_DATAGRAIL_CUSTOMER_ID;
  const containerId = process.env.REACT_APP_DATAGRAIL_CONTAINER_ID;

  if (!customerId || !containerId || window.location.href.includes("embedded-widget")) {
    return;
  }

  const script = document.createElement("script");
  script.src = `https://api.consentjs.datagrail.io/${customerId}/${containerId}/consent-loader.js`;
  script.async = true;
  document.head.appendChild(script);
};

export const isConsentManagerActive = (): boolean => {
  return Boolean(window.DG_BANNER_API);
};

export const showConsentPreferences = (): void => {
  const api = window.DG_BANNER_API;

  if (typeof api?.showLayer === "function") {
    api.showLayer(DATAGRAIL_PREFERENCE_LAYER_ID);
    return;
  }

  api?.showConsentBanner?.();
};
