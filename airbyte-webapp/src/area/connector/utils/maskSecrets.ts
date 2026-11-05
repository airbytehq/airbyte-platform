const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Recursively masks values in a configuration object based on the schema's airbyte_secret property
 */
export const maskSecrets = (
  config: Record<string, unknown>,
  schema: Record<string, unknown>
): Record<string, unknown> => {
  // Mask with every oneOf option, not only the selected one. A schema can declare its discriminator in several ways,
  // and an option that is not recognized must not leave a secret unmasked. The server masks secrets the same way.
  const oneOfOptions = Array.isArray(schema.oneOf) ? schema.oneOf.filter(isPlainObject) : [];
  const maskedConfig = oneOfOptions.reduce<Record<string, unknown>>(
    (masked, option) => maskSecrets(masked, option),
    // Create a copy of the config to avoid mutating the original
    { ...config }
  );

  // If schema doesn't have properties, return the config as is
  if (!schema.properties || typeof schema.properties !== "object") {
    return maskedConfig;
  }

  // Iterate through each property in the schema
  Object.entries(schema.properties as Record<string, unknown>).forEach(([key, propertySchema]) => {
    // Skip if the property doesn't exist in the config
    if (!(key in maskedConfig)) {
      return;
    }

    // Handle nested objects recursively
    if (
      propertySchema &&
      typeof propertySchema === "object" &&
      (propertySchema as Record<string, unknown>).type === "object" &&
      (propertySchema as Record<string, unknown>).properties &&
      typeof maskedConfig[key] === "object" &&
      maskedConfig[key] !== null
    ) {
      maskedConfig[key] = maskSecrets(
        maskedConfig[key] as Record<string, unknown>,
        propertySchema as Record<string, unknown>
      );
      return;
    }

    // Handle oneOf in property schema
    if (
      propertySchema &&
      typeof propertySchema === "object" &&
      (propertySchema as Record<string, unknown>).oneOf &&
      typeof maskedConfig[key] === "object" &&
      maskedConfig[key] !== null
    ) {
      maskedConfig[key] = maskSecrets(
        maskedConfig[key] as Record<string, unknown>,
        propertySchema as Record<string, unknown>
      );
      return;
    }

    // Handle arrays whose items have their own schema, unless the whole array is a secret
    const value = maskedConfig[key];
    const itemSchema = isPlainObject(propertySchema) && !propertySchema.airbyte_secret && propertySchema.items;
    if (Array.isArray(value) && isPlainObject(itemSchema)) {
      maskedConfig[key] = value.map((item) => (isPlainObject(item) ? maskSecrets(item, itemSchema) : item));
      return;
    }

    // Mask the value if it's marked as a secret
    if (
      propertySchema &&
      typeof propertySchema === "object" &&
      (propertySchema as Record<string, unknown>).airbyte_secret === true &&
      maskedConfig[key] !== null &&
      maskedConfig[key] !== undefined &&
      maskedConfig[key] !== ""
    ) {
      maskedConfig[key] = "******";
    }
  });

  return maskedConfig;
};
