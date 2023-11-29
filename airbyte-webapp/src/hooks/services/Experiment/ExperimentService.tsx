import type { Experiments } from "./experiments";

import { createContext, useContext, useEffect, useMemo } from "react";
import { useObservable } from "react-use";
import { EMPTY, Observable } from "rxjs";

import { isDevelopment } from "core/utils/isDevelopment";

export type ContextKind =
  | "user"
  | "organization"
  | "workspace"
  | "connection"
  | "source"
  | "destination"
  | "source-definition"
  | "destination-definition";

const devOverwrites = process.env.REACT_APP_EXPERIMENT_OVERWRITES
  ? (process.env.REACT_APP_EXPERIMENT_OVERWRITES as unknown as Record<string, unknown>)
  : {};

const experimentContext = createContext<ExperimentService | null>(null);

/**
 * An ExperimentService must be able to give us a value for a given key of an experiment
 * as well as update us about changes in the experiment.
 */
export interface ExperimentService {
  addContext: (kind: ContextKind, key: string) => void;
  removeContext: (kind: Exclude<ContextKind, "user">) => void;
  getExperiment<K extends keyof Experiments>(key: K, defaultValue: Experiments[K]): Experiments[K];
  getExperimentChanges$<K extends keyof Experiments>(key: K): Observable<Experiments[K]>;
}

const debugContext = isDevelopment() ? (msg: string) => console.debug(`%c${msg}`, "color: SlateBlue") : () => undefined;

/**
 * Registers a context with the experiment service (usually the LaunchDarkly client on Cloud),
 * potentially causing new flags to be fetched. The context will be removed when the component unmounts,
 * or when a falsy key is passed.
 */
export const useExperimentContext = (kind: Exclude<ContextKind, "user">, key: string | undefined) => {
  const experimentService = useContext(experimentContext);

  useEffect(() => {
    if (!experimentService) {
      // We're not running inside any experiment service so simply return;
      return;
    }
    if (key) {
      debugContext(`[Experiments] Registering context ${kind} with key ${key}`);
      experimentService.addContext(kind, key);
    } else {
      debugContext(`[Experiments] Removing context ${kind} due to empty key`);
      experimentService.removeContext(kind);
    }

    return () => {
      debugContext(`[Experiments] Removing context ${kind}`);
      experimentService.removeContext(kind);
    };
  }, [kind, key, experimentService]);
};

function useExperimentHook<K extends keyof Experiments>(key: K, defaultValue: Experiments[K]): Experiments[K] {
  const experimentService = useContext(experimentContext);
  // Get the observable for the changes of the experiment or an empty (never emitting) observable in case the
  // experiment service doesn't exist (e.g. we're running in OSS or it failed to initialize)
  const onChanges$ = useMemo(() => experimentService?.getExperimentChanges$(key) ?? EMPTY, [experimentService, key]);
  // Listen to changes on that observable and use the current value (if the service exist) or the defaultValue otherwise
  // as the starting value.
  return useObservable(onChanges$, experimentService?.getExperiment(key, defaultValue) ?? defaultValue);
}

function useExperimentWithOverwrites<K extends keyof Experiments>(
  key: K,
  defaultValue: Experiments[K]
): Experiments[K] {
  // Load the regular experiments value via the prod hook
  const value = useExperimentHook(key, defaultValue);
  // Use the overwrite value if it's available, otherwise the proper value
  return key in devOverwrites ? (devOverwrites[key] as Experiments[K]) : value;
}

// Allow overwriting values via the .experiments.dev file (and thus the REACT_APP_EXPERIMENT_OVERWRITES env variable) only during development
const isCypress = window.hasOwnProperty("Cypress");
export const useExperiment =
  !isCypress && process.env.NODE_ENV === "development" ? useExperimentWithOverwrites : useExperimentHook;

export const ExperimentProvider = experimentContext.Provider;
