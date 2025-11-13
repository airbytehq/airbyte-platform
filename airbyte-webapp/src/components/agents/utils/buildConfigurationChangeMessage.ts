interface ConfigurationChangeParams {
  currentFormValues: Record<string, unknown>;
  previousFormValues: Record<string, unknown>;
}

/**
 * Builds a formatted message describing configuration changes between form snapshots.
 * Note: Form values should already have secrets masked before being passed to this function.
 */
export const buildConfigurationChangeMessage = ({
  currentFormValues,
  previousFormValues,
}: ConfigurationChangeParams): string => {
  const currentName = currentFormValues.name as string | undefined;
  const previousName = previousFormValues.name as string | undefined;
  const hasNameChange = currentName && currentName !== previousName;

  const currentConfig = currentFormValues.connectionConfiguration as Record<string, unknown> | undefined;
  const previousConfig = previousFormValues.connectionConfiguration as Record<string, unknown> | undefined;

  const nameChangeSection = hasNameChange
    ? `
**Name changed:**
- Old: ${previousName || "(empty)"}
- New: ${currentName}
`
    : "";

  const configChangeSection = currentConfig
    ? `
**Configuration changed:**

*Previous configuration:*
\`\`\`json
${JSON.stringify(previousConfig || {}, null, 2)}
\`\`\`

*New configuration:*
\`\`\`json
${JSON.stringify(currentConfig, null, 2)}
\`\`\`
`
    : "";

  return `I've made some changes to the configuration in the form:
${nameChangeSection}${configChangeSection}
Please tell me an overview of which changes you see that I made, and ask me what I would want to do next, with some suggestions. Do not recap the full configuration state, just the changes. Start with something like "I noticed you've made changes to the configuration".`;
};
