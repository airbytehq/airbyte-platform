import { z } from "zod";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

const SourceNamespaceDefinitionTypeSchema = z.object({
  namespaceDefinition: z.literal(NamespaceDefinitionType.source),
  namespaceFormat: z.string().optional(),
});

const DestinationNamespaceDefinitionTypeSchema = z.object({
  namespaceDefinition: z.literal(NamespaceDefinitionType.destination),
  namespaceFormat: z.string().optional(),
});

const CustomFormatNamespaceDefinitionTypeSchema = z.object({
  namespaceDefinition: z.literal(NamespaceDefinitionType.customformat),
  namespaceFormat: z.string().trim().nonempty("form.empty.error"),
});

export const namespaceFormatSchema = z.discriminatedUnion("namespaceDefinition", [
  SourceNamespaceDefinitionTypeSchema,
  DestinationNamespaceDefinitionTypeSchema,
  CustomFormatNamespaceDefinitionTypeSchema,
]);
