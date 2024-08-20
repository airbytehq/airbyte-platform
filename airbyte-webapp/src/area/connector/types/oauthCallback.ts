interface CompletedOAuthEvent {
  type: "completed";
  query: Record<string, unknown>;
}

interface TakeoverOAuthEvent {
  type: "takeover";
}

interface CancelOAuthEvent {
  type: "cancel";
  tabUuid: string;
}

export type OAuthEvent = CompletedOAuthEvent | TakeoverOAuthEvent | CancelOAuthEvent;
