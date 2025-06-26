import { BuildConfig } from "./types";

export const buildConfig: BuildConfig = {
  apiUrl: process.env.REACT_APP_API_URL ?? "",
  keycloakBaseUrl: process.env.REACT_APP_KEYCLOAK_BASE_URL ?? window.location.origin,
};

export class MissingConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MissingConfigError";
  }
}
