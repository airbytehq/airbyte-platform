import { FormBuildError } from "core/form/FormBuildError";
import { SectionDetails } from "core/form/types";

export const DEFAULT_GROUP_ID = "default";
export const ADVANCED_GROUP_ID = "advanced";

const isStringList = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((item) => typeof item === "string");

const isAbsentOrOfType = (value: unknown, type: "string" | "number" | "boolean") =>
  value === undefined || typeof value === type;

const isSectionDetails = (value: unknown): value is SectionDetails => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const { id, title, description, order, check, groups } = value as Record<string, unknown>;

  const hasRequiredFields = typeof id === "string" && typeof title === "string" && isStringList(groups);
  const hasValidOptionalFields =
    isAbsentOrOfType(description, "string") && isAbsentOrOfType(order, "number") && isAbsentOrOfType(check, "boolean");
  return hasRequiredFields && hasValidOptionalFields;
};

const hasNoDuplicates = (values: string[]) => new Set(values).size === values.length;

/**
 * Section ids must be unique. Each group can belong to only one section, so a group listed twice makes the section
 * order ambiguous, and each listed group must exist.
 */
const isSectionList = (value: unknown, knownGroupIds: Set<string>): value is SectionDetails[] => {
  if (!Array.isArray(value) || value.length === 0 || !value.every(isSectionDetails)) {
    return false;
  }

  const sectionGroupIds = value.flatMap((section) => section.groups);
  return (
    hasNoDuplicates(value.map((section) => section.id)) &&
    hasNoDuplicates(sectionGroupIds) &&
    sectionGroupIds.every((groupId) => knownGroupIds.has(groupId))
  );
};

// Sections with an order come first, sorted by it. Sections without one keep the order they are listed in.
const compareSectionOrder = (a: SectionDetails, b: SectionDetails) => {
  if (a.order === undefined && b.order === undefined) {
    return 0;
  }
  if (a.order === undefined) {
    return 1;
  }
  if (b.order === undefined) {
    return -1;
  }
  return a.order - b.order;
};

/**
 * Reads the `sections` of a connector spec and returns them in display order, or undefined when the spec has none.
 * Throws a `FormBuildError` when the sections are not valid.
 */
export function getSpecSections(connectionSpecification: unknown): SectionDetails[] | undefined {
  if (!connectionSpecification || typeof connectionSpecification !== "object") {
    return undefined;
  }
  const spec = connectionSpecification as Record<string, unknown>;
  if (!("sections" in spec)) {
    return undefined;
  }

  // The webapp puts fields without a group in the default group and the resource allocation field in the advanced
  // group, so sections can list those groups without the spec declaring them
  const specGroupIds = Array.isArray(spec.groups)
    ? spec.groups.flatMap((group) => (typeof group?.id === "string" ? [group.id] : []))
    : [];
  const knownGroupIds = new Set([...specGroupIds, DEFAULT_GROUP_ID, ADVANCED_GROUP_ID]);
  if (!isSectionList(spec.sections, knownGroupIds)) {
    throw new FormBuildError("connectorForm.error.invalidSections");
  }

  return spec.sections
    .map(({ id, title, description, order, check, groups }) => ({ id, title, description, order, check, groups }))
    .sort(compareSectionOrder);
}
