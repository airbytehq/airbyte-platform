import { useEffect } from "react";
import { useShallowCompareEffect } from "react-use";

import { AnalyticsService } from "./AnalyticsService";
import { EventParams } from "./types";

type AnalyticsContext = Record<string, unknown>;

const analyticsService = new AnalyticsService();

export const useAnalyticsService = (): AnalyticsService => {
  return analyticsService;
};

export const useAnalyticsIdentifyUser = (userId?: string, traits?: Record<string, unknown>): void => {
  useEffect(() => {
    if (userId) {
      analyticsService.identify(userId, traits);
    }
  }, [traits, userId]);
};

export const useTrackPage = (page: string, params: EventParams = {}): void => {
  useShallowCompareEffect(() => {
    analyticsService.page(page, params);
  }, [page, params]);
};

export const useAnalyticsRegisterValues = (props?: AnalyticsContext | null): void => {
  useEffect(() => {
    if (!props) {
      return;
    }

    analyticsService.setContext(props);
    return () => analyticsService.removeFromContext(...Object.keys(props));
  }, [props]);
};
