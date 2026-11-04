import { LDMultiKindContext, LDSingleKindContext } from "launchdarkly-js-client-sdk";

import { UserRead } from "core/api/types/AirbyteClient";
import { ContextKind } from "core/services/Experiment";

export function createLDContext(kind: ContextKind, key: string): LDSingleKindContext {
  return {
    kind,
    key,
  };
}

export function createUserContext(user: UserRead | null, locale: string): LDSingleKindContext {
  const kind = "user";

  if (!user) {
    return {
      kind,
      anonymous: true,
      locale,
      // No key: LaunchDarkly generates and stores one for anonymous contexts.
    };
  }

  return {
    kind,
    anonymous: false,
    key: user.userId,
    email: user.email,
    name: user.name,
    locale,
  };
}

export function createMultiContext(...args: LDSingleKindContext[]): LDMultiKindContext {
  const multiContext: LDMultiKindContext = {
    kind: "multi",
  };

  args.forEach((context) => {
    multiContext[context.kind] = context;
  });

  return multiContext;
}

export function isMultiContext(context: LDMultiKindContext | LDSingleKindContext): context is LDMultiKindContext {
  return context.kind === "multi";
}

export function getSingleContextsFromMulti(multiContext: LDMultiKindContext): LDSingleKindContext[] {
  return Object.entries(multiContext)
    .filter(([key]) => key !== "kind")
    .map(([_, context]) => context as LDSingleKindContext);
}
