/**
 * Recursively masks values in a configuration object based on the schema's airbyte_secret property
 */
export const maskSecrets = (
  config: Record<string, unknown>,
  schema: Record<string, unknown>
): Record<string, unknown> => {
  // Create a copy of the config to avoid mutating the original
  const maskedConfig = { ...config };

  // Handle oneOf schemas
  if (schema.oneOf && Array.isArray(schema.oneOf)) {
    // Find the matching schema in oneOf based on discriminator
    // In JSON Schema, discriminator fields are often defined with 'const' values
    const oneOfSchemas = schema.oneOf as Array<Record<string, unknown>>;

    // Try to find a discriminator field by looking for properties with 'const' values
    for (const subSchema of oneOfSchemas) {
      if (typeof subSchema === "object" && subSchema.properties && typeof subSchema.properties === "object") {
        const properties = subSchema.properties as Record<string, unknown>;

        // Check each property to see if it's a potential discriminator
        let matchingSchema = null;
        for (const [propKey, propSchema] of Object.entries(properties)) {
          if (
            typeof propSchema === "object" &&
            propSchema !== null &&
            "const" in propSchema &&
            propKey in config &&
            config[propKey] === propSchema.const
          ) {
            // Found a matching schema based on the discriminator
            matchingSchema = subSchema;
            break;
          }
        }

        if (matchingSchema) {
          // Apply masking using the matching schema
          return maskSecrets(config, matchingSchema);
        }
      }
    }
  }

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
