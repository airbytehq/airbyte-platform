import React, { useCallback, useContext, useMemo, useState } from "react";

import { InstanceConfigurationResponse, InstanceConfigurationResponseEdition } from "core/api/types/AirbyteClient";

import { FeatureItem, FeatureSet } from "./types";

interface FeatureServiceContext {
  features: FeatureItem[];
  setFeatureOverwrites: (features: FeatureItem[] | FeatureSet | undefined) => void;
}

const featureServiceContext = React.createContext<FeatureServiceContext | null>(null);

const featureSetFromList = (featureList: FeatureItem[]): FeatureSet => {
  return featureList.reduce((set, val) => ({ ...set, [val]: true }), {} as FeatureSet);
};

const featureSetFromInstanceConfig = (instanceConfig: InstanceConfigurationResponse): FeatureSet => {
  return {
    [FeatureItem.KeycloakAuthentication]: !!instanceConfig.auth,
    [FeatureItem.MultiWorkspaceUI]: instanceConfig.edition === InstanceConfigurationResponseEdition.pro,
    [FeatureItem.RBAC]: instanceConfig.edition === InstanceConfigurationResponseEdition.pro,
    [FeatureItem.EnterpriseBranding]: instanceConfig.edition === InstanceConfigurationResponseEdition.pro,
    [FeatureItem.APITokenManagement]: instanceConfig.edition === InstanceConfigurationResponseEdition.pro,
  };
};

interface FeatureServiceProps {
  features: FeatureItem[];
  instanceConfig?: InstanceConfigurationResponse;
}

/**
 * The FeatureService allows tracking support for whether a specific feature should be
 * enabled or disabled. The feature can be enabled/disabled on either of the following level:
 *
 * - globally (the values passed into this service)
 * - workspace (can be configured via setWorkspaceFeatures)
 * - user (can be configured via setUserFeatures)
 *
 * In addition via setFeatureOverwrites allow overwriting any features. The priority for configuring
 * features is: overwrite > user > workspace > globally, i.e. if a feature is disabled for a user
 * it will take precedence over the feature being enabled globally or for that workspace.
 */
export const FeatureService: React.FC<React.PropsWithChildren<FeatureServiceProps>> = ({
  features: defaultFeatures,
  instanceConfig,
  children,
}) => {
  const [overwrittenFeatures, setOverwrittenFeaturesState] = useState<FeatureSet>();

  const envOverwrites = useMemo(() => {
    // Allow env feature overwrites only during development
    if (process.env.NODE_ENV !== "development") {
      return {};
    }
    const featureSet: FeatureSet = {};
    for (const item of Object.values(FeatureItem)) {
      // We can't access process.env with Vite usually with a dynamic value (see https://vitejs.dev/guide/env-and-mode.html)
      // since Vite find and replace that during build, i.e. it will only work in development mode.
      // Since this code is only used during development mode and anyway stripped in production with the above `if` guard,
      // we can do this here. This would not work if we'd want to do the same also in production code!
      const envFeature = process.env[`REACT_APP_FEATURE_${item}`];
      // If a REACT_APP_FEATURE_{id} env variable is set it can overwrite that feature state
      if (envFeature) {
        featureSet[item] = envFeature === "true";
      }
    }
    return featureSet;
  }, []);

  const combinedFeatures = useMemo(() => {
    const combined: FeatureSet = {
      ...featureSetFromList(defaultFeatures),
      ...(instanceConfig ? featureSetFromInstanceConfig(instanceConfig) : {}),
      ...overwrittenFeatures,
      ...envOverwrites,
    };

    return Object.entries(combined)
      .filter(([, enabled]) => enabled)
      .map(([id]) => id) as FeatureItem[];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [overwrittenFeatures, instanceConfig, ...defaultFeatures]);

  const setFeatureOverwrites = useCallback((features: FeatureItem[] | FeatureSet | undefined) => {
    setOverwrittenFeaturesState(Array.isArray(features) ? featureSetFromList(features) : features);
  }, []);

  const serviceContext = useMemo(
    (): FeatureServiceContext => ({
      features: combinedFeatures,
      setFeatureOverwrites,
    }),
    [combinedFeatures, setFeatureOverwrites]
  );

  return <featureServiceContext.Provider value={serviceContext}>{children}</featureServiceContext.Provider>;
};

export const useFeatureService: () => FeatureServiceContext = () => {
  const featureService = useContext(featureServiceContext);
  if (!featureService) {
    throw new Error("useFeatureService must be used within a FeatureService.");
  }
  return featureService;
};

/**
 * Returns whether a specific feature is enabled currently.
 * Will cause the component to rerender if the state of the feature changes.
 */
export const useFeature = (feature: FeatureItem): boolean => {
  const { features } = useFeatureService();
  return features.includes(feature);
};

export const IfFeatureEnabled: React.FC<React.PropsWithChildren<{ feature: FeatureItem }>> = ({
  feature,
  children,
}) => {
  const hasFeature = useFeature(feature);
  return hasFeature ? <>{children}</> : null;
};
