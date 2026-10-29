import { FormBlock, SectionDetails } from "core/form/types";

import { DEFAULT_GROUP_ID } from "./specSections";

/**
 * Assigns each root-level form block to the section that lists its group.
 * Blocks whose group is in no section, such as the connector name, go to the last section.
 */
export function splitSections(blocks: FormBlock[], sections: SectionDetails[]): FormBlock[][] {
  const lastSectionIndex = sections.length - 1;
  const sectionIndexByGroupId = new Map(
    sections.flatMap((section, sectionIndex) => section.groups.map((groupId) => [groupId, sectionIndex] as const))
  );

  const blocksBySection = sections.map((): FormBlock[] => []);
  blocks.forEach((block) => {
    const sectionIndex = sectionIndexByGroupId.get(block.group || DEFAULT_GROUP_ID) ?? lastSectionIndex;
    blocksBySection[sectionIndex].push(block);
  });
  return blocksBySection;
}

/**
 * The form paths to validate for a section. An object without properties only holds the OAuth button,
 * and its path is the whole connector configuration, so it is left out.
 */
export function getSectionPaths(blocks: FormBlock[]): string[] {
  return blocks
    .filter((block) => !(block._type === "formGroup" && block.properties.length === 0))
    .map((block) => block.path);
}

/**
 * Flattens a react-hook-form error tree into the paths of the fields that have an error.
 */
export function getErrorPaths(errors: unknown, path: string): string[] {
  if (!errors || typeof errors !== "object") {
    return [];
  }
  if ("type" in errors && typeof errors.type === "string") {
    return [path];
  }
  return Object.entries(errors).flatMap(([key, error]) => getErrorPaths(error, path ? `${path}.${key}` : key));
}

/**
 * Whether the path is the parent path or a path nested under it. A path that only shares a prefix, such as `host` and
 * `hostname`, is not nested.
 */
export const isSameOrChildPath = (parent: string, path: string) => path === parent || path.startsWith(`${parent}.`);

export const isPathInSection = (sectionPaths: string[], path: string) =>
  sectionPaths.some((sectionPath) => isSameOrChildPath(sectionPath, path));
