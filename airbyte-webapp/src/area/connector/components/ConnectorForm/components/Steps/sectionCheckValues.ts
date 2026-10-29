import { CHECK_SECTION_KEY, FormBlock, SectionDetails } from "core/form/types";

import { ConnectorFormValues } from "../../types";

const CONNECTOR_CONFIG_PATH_PREFIX = "connectionConfiguration.";

/**
 * The top-level connector config keys that the given blocks hold.
 */
export function getConnectorConfigKeys(blocks: FormBlock[]): string[] {
  return blocks
    .filter((block) => block.path.startsWith(CONNECTOR_CONFIG_PATH_PREFIX))
    .map((block) => block.path.slice(CONNECTOR_CONFIG_PATH_PREFIX.length).split(".")[0]);
}

/**
 * The values to check a section with. The connector config holds the fields of the section and of the sections before
 * it, never those of later sections, and names the section under `check-section`.
 */
export function getSectionCheckValues(
  values: ConnectorFormValues,
  sections: SectionDetails[],
  blocksBySection: FormBlock[][],
  sectionIndex: number
): ConnectorFormValues {
  const laterSectionConfigKeys = new Set(blocksBySection.slice(sectionIndex + 1).flatMap(getConnectorConfigKeys));
  const connectionConfiguration = Object.fromEntries(
    Object.entries(values.connectionConfiguration).filter(([key]) => !laterSectionConfigKeys.has(key))
  );
  return {
    ...values,
    connectionConfiguration: { ...connectionConfiguration, [CHECK_SECTION_KEY]: sections[sectionIndex].id },
  };
}
