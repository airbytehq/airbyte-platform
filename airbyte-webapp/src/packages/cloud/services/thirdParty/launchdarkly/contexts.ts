import { LDMultiKindContext, LDSingleKindContext } from "launchdarkly-js-client-sdk";

import { UserRead } from "core/api/types/AirbyteClient";
import { ContextKind } from "hooks/services/Experiment";

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
      // @ts-expect-error LaunchDarkly's typescript types specify that key must be defined, but actually they handle a null or undefined key. It's even recommended in their docs to omit the key for anonymous users https://docs.launchdarkly.com/sdk/client-side/javascript/migration-2-to-3#understanding-changes-to-anonymous-users
      key: undefined,
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
