type FullStoryGlobal = (method: "getSession", options: { format: "url.now" }) => string;

export const fullStorySessionLink = () =>
  (window as { FS?: FullStoryGlobal }).FS?.("getSession", { format: "url.now" });
