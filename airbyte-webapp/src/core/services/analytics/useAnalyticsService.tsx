import throttle from "lodash/throttle";
import { useCallback, useEffect } from "react";
import { useShallowCompareEffect } from "react-use";

import { useWebappConfig } from "core/config";

import { AnalyticsService } from "./AnalyticsService";
import { Action, EventParams, Namespace } from "./types";

type AnalyticsContext = Record<string, unknown>;

const analyticsService = new AnalyticsService();

export const useAnalyticsService = (): AnalyticsService => {
  const config = useWebappConfig();

  useEffect(() => {
    if (!analyticsService.hasContext("airbyte_version")) {
      analyticsService.setContext({
        airbyte_version: config.version,
        environment: config.version === "dev" ? "dev" : "prod",
      });
    }
  }, [config.version]);

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

const useThrottledTrack = (
  analyticsService: AnalyticsService,
  namespace: Namespace,
  params: Record<string, unknown>,
  throttleTime: number = 50
) => {
  const throttledTrackFunction = throttle(
    (action: Action) => {
      analyticsService.track(namespace, action, params);
    },
    throttleTime,
    { leading: true, trailing: false }
  );

  return useCallback(throttledTrackFunction, [throttledTrackFunction]);
};

export const useTrackMount = ({
  namespace,
  mountAction,
  unmountAction,
  params,
}: {
  namespace: Namespace;
  mountAction: Action;
  unmountAction: Action;
  params: Record<string, unknown>;
}) => {
  const analyticsService = useAnalyticsService();
  const throttledTrack = useThrottledTrack(analyticsService, namespace, params);

  useEffect(() => {
    throttledTrack(mountAction);
    return () => throttledTrack(unmountAction);
  }, [throttledTrack, mountAction, unmountAction]);
};
