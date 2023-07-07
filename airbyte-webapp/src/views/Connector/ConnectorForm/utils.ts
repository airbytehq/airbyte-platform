import toLower from "lodash/toLower";

import { FormBaseItem, FormBlock } from "core/form/types";
import { AdvancedAuth, SourceAuthSpecification } from "core/request/AirbyteClient";
import { naturalComparator } from "utils/objects";

import { ConnectorDefinitionSpecification } from "../../../core/domain/connector";

export function makeConnectionConfigurationPath(path: string[] = []): string {
  return ["connectionConfiguration", ...path].join(".");
}

export type Path = Array<string | number>;

export const isNumerical = (input: string | number): boolean => {
  return typeof input === "number" || /^\d+$/.test(input);
};

/**
 * Takes an array of strings or numbers and remove all elements from it that are either
 * a number or a string that just contains a number. This will be used to remove index
 * accessors into oneOf from paths, since they are not part of the field path later.
 */
export const stripNumericalEntries = (paths: Path): string[] => {
  return paths.filter((p): p is string => !isNumerical(p));
};

export function authPredicateMatchesPath(
  path: string,
  spec?: { authSpecification?: SourceAuthSpecification; advancedAuth?: AdvancedAuth }
) {
  const advancedAuth = spec?.advancedAuth;
  const legacyOauthSpec = spec?.authSpecification?.oauth2Specification;

  // If OAuth is not configured at all for this spec, then there is no predicate to match on
  if (!advancedAuth && !legacyOauthSpec) {
    return false;
  }

  const authPredicate = advancedAuth
    ? advancedAuth.predicateKey && makeConnectionConfigurationPath(advancedAuth.predicateKey)
    : legacyOauthSpec && makeConnectionConfigurationPath(stripNumericalEntries(legacyOauthSpec.rootObject as Path));

  return path === (authPredicate ?? makeConnectionConfigurationPath());
}

type OAuthOutputSpec = { properties: Record<string, { type: string; path_in_connector_config: string[] }> } | undefined;

export function serverProvidedOauthPaths(
  connector?: ConnectorDefinitionSpecification
): Record<string, { path_in_connector_config: string[] }> {
  return {
    ...((connector?.advancedAuth?.oauthConfigSpecification?.completeOAuthOutputSpecification as OAuthOutputSpec)
      ?.properties ?? {}),
    ...((connector?.advancedAuth?.oauthConfigSpecification?.completeOAuthServerOutputSpecification as OAuthOutputSpec)
      ?.properties ?? {}),
  };
}

export function OrderComparator(a: FormBlock, b: FormBlock): number {
  const aIsNumber = Number.isInteger(a.order);
  const bIsNumber = Number.isInteger(b.order);
  // Treat being a formCondition as required, because a value must be selected for it regardless of being optional or required
  // Treat formGroup as required, because the optional/required validations only apply to the nested fields inside of it
  // Treat const values as required, since they aren't rendered anyway and otherwise can mess up ordering of optional fields
  const aIsRequired =
    a.isRequired || a._type === "formCondition" || a._type === "formGroup" || a.always_show || a.const !== undefined;
  const bIsRequired =
    b.isRequired || b._type === "formCondition" || b._type === "formGroup" || b.always_show || b.const !== undefined;

  switch (true) {
    case aIsRequired && !bIsRequired:
      return -1;
    case !aIsRequired && bIsRequired:
      return 1;
    case aIsNumber && bIsNumber:
      return (a.order as number) - (b.order as number);
    case aIsNumber && !bIsNumber:
      return -1;
    case bIsNumber && !aIsNumber:
      return 1;
    default:
      return naturalComparator(a.fieldKey, b.fieldKey);
  }
}

export function getPatternDescriptor(formItem: FormBaseItem): string | undefined {
  if (formItem.pattern_descriptor !== undefined) {
    return formItem.pattern_descriptor;
  }
  if (formItem.pattern === undefined) {
    return undefined;
  }
  if (formItem.pattern.includes("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")) {
    return "YYYY-MM-DDTHH:mm:ssZ";
  }
  if (formItem.pattern.includes("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$")) {
    return "YYYY-MM-DDTHH:mm:ss";
  }
  if (formItem.pattern.includes("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$")) {
    return "YYYY-MM-DD HH:mm:ss";
  }
  if (formItem.pattern.includes("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")) {
    return "YYYY-MM-DD";
  }
  if (formItem.pattern.includes("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$")) {
    return "YYYY-MM-DDTHH:mm:ss.SSSZ";
  }
  return undefined;
}

export function isLocalhost(value: string | undefined) {
  if (value === undefined) {
    return false;
  }
  const normalized = toLower(value.trim());
  return normalized === "localhost" || /127\.\d+\.\d+\.\d+/.test(normalized);
}
