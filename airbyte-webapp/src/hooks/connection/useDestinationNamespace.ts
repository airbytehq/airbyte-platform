import { useIntl } from "react-intl";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

interface NamespaceOptions {
  namespaceDefinition:
    | typeof NamespaceDefinitionType.source
    | typeof NamespaceDefinitionType.destination
    | typeof NamespaceDefinitionType.customformat;
  namespaceFormat?: string;
}

export const useDestinationNamespace = (opt: NamespaceOptions, sourceNamespace?: string): string | undefined => {
  const { formatMessage } = useIntl();

  switch (opt.namespaceDefinition) {
    case NamespaceDefinitionType.source:
      return sourceNamespace ?? formatMessage({ id: "connection.catalogTree.destinationSchema" });
    case NamespaceDefinitionType.destination:
      return formatMessage({ id: "connection.catalogTree.destinationSchema" });
    case NamespaceDefinitionType.customformat:
      const customString = opt.namespaceFormat?.replace(
        // we _actually_ want to find that template string and replace it in this case
        // eslint-disable-next-line no-template-curly-in-string
        "${SOURCE_NAMESPACE}",
        sourceNamespace ?? ""
      );

      return customString && customString.length > 0
        ? customString
        : formatMessage({ id: "connection.catalogTree.destinationSchema" });
  }
};
