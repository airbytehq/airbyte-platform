import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

export const namespaceDefinitionOptions: Record<NamespaceDefinitionType, string> = {
  [NamespaceDefinitionType.destination]: "destinationFormat",
  [NamespaceDefinitionType.source]: "sourceFormat",
  [NamespaceDefinitionType.customformat]: "customFormat",
};
