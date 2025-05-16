import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef } from "react";
import { useBlocker as unstable_useBlocker } from "react-router-dom";

export type Blocker = Extract<ReturnType<typeof unstable_useBlocker>, { state: "blocked" }>;
export type BlockerFn = (blocker: Blocker) => void;

const context = createContext<{ setBlocker: (blockerFn: BlockerFn | undefined) => void } | null>(null);

/**
 * The BlockerService is a wrapping service that allows us to use react router's `useBlocker` multiple
 * times in our codebase, which it by default forbids. This service will be in charge of the one `useBlocker`
 * call to react router and our custom `useBlocker` call will use this service to pass the actual blocker along.
 */
export const BlockerService: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const currentBlockerRef = useRef<BlockerFn>();
  // Assume we want to block if there's a blocker currently set in the provider
  const blocker = unstable_useBlocker(() => !!currentBlockerRef.current);

  // Once the actual blocker state changes to `blocked` we call the blocker callback
  // which could show a confirmation modal, and pass the blocker along, so it can
  // release the block calling .proceed() on it.
  useEffect(() => {
    if (blocker.state === "blocked") {
      if (currentBlockerRef.current) {
        currentBlockerRef.current(blocker);
      } else {
        // If we're running into a blocked state on the blocker but don't have a currentBlockerRef anymore,
        // it means we're trying to navigate away (and got blocked) in the same render cycle as the blocker was already removed.
        // In this case it's safe to just release the block immediately and proceed with the navigation.
        blocker.proceed();
      }
    }
  }, [blocker, blocker.state]);

  const setBlocker = useCallback((blockerFn: BlockerFn | undefined) => {
    // If a blocker is tried to be activated while another one is already active
    // throw an error, since this is currently not supported.
    if (blockerFn && currentBlockerRef.current) {
      throw new Error("Tried to activate a new blocker while a blocker is already active.");
    }
    currentBlockerRef.current = blockerFn;
  }, []);

  const ctx = useMemo(
    () => ({
      setBlocker,
    }),
    [setBlocker]
  );
  return <context.Provider value={ctx}>{children}</context.Provider>;
};

export const useBlocker = (blocker: (blocker: Blocker) => void, shouldBlock = true) => {
  const blockerContext = useContext(context);

  if (!blockerContext) {
    throw new Error("Can't use useBlocker outside BlockerService.");
  }

  useEffect(() => {
    if (!shouldBlock) {
      return;
    }

    blockerContext.setBlocker(blocker);
    return () => {
      blockerContext.setBlocker(undefined);
    };
  }, [blocker, blockerContext, shouldBlock]);
};
