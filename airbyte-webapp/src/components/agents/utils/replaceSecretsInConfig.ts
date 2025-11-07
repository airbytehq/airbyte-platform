import { type SecretsMap } from "../../agents/types";

/**
 * Injects secret values into a configuration object based on their paths.
 * Secrets are stored with dot-separated paths (e.g., "credentials.api_key") and injected
 * at the corresponding location in the config object.
 */
export const replaceSecretsInConfig = (config: unknown, secrets: SecretsMap): unknown => {
  // Deep clone the config to avoid mutations
  const result = JSON.parse(JSON.stringify(config)) as Record<string, unknown>;

  // Walk through each secret and inject it at its path
  for (const [path, value] of secrets.entries()) {
    const pathParts = path.split(".");
    let current: Record<string, unknown> = result;

    // Navigate to the parent of the target field
    for (let i = 0; i < pathParts.length - 1; i++) {
      const part = pathParts[i];
      if (current[part] === undefined) {
        current[part] = {};
      }
      current = current[part] as Record<string, unknown>;
    }

    // Set the value at the final key
    const finalKey = pathParts[pathParts.length - 1];
    current[finalKey] = value;
  }

  return result;
};
