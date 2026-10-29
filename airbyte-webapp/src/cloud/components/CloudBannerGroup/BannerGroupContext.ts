import { createContext } from "react";

export interface BannerGroupContextValue {
  registerBanner: (id: string) => void;
  unregisterBanner: (id: string) => void;
  checkIsBannerHidden: (id: string) => boolean;
}

export const BannerGroupContext = createContext<BannerGroupContextValue>({
  registerBanner: () => undefined,
  unregisterBanner: () => undefined,
  checkIsBannerHidden: () => false,
});
