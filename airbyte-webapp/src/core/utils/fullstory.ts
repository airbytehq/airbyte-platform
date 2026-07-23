interface FullStoryGlobal {
  (method: "getSession", options: { format: "url.now" }): string;
  (method: "setIdentity", options: { uid: string; properties?: Record<string, unknown> }): void;
  (method: "setProperties", options: { type: "user" | "page"; properties: Record<string, unknown> }): void;
}

const getFS = () => (window as { FS?: FullStoryGlobal }).FS;

export const fullStorySessionLink = () => getFS()?.("getSession", { format: "url.now" });

export const fullStorySetIdentity = (uid: string, properties?: Record<string, unknown>) =>
  getFS()?.("setIdentity", { uid, ...(properties && { properties }) });

export const fullStorySetUserProperties = (properties: Record<string, unknown>) =>
  getFS()?.("setProperties", { type: "user", properties });
