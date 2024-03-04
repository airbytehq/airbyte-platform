import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";

import { DestinationNamespaceFormValues } from "./DestinationNamespaceModal";
import { ExampleSettingsTable } from "./ExampleSettingsTable";

export const DestinationNamespaceDescription = () => {
  const { watch } = useFormContext<DestinationNamespaceFormValues>();
  const watchedNamespaceDefinition = watch("namespaceDefinition");

  return (
    <>
      <FormattedMessage
        id={`connectionForm.modal.destinationNamespace.option.${
          watchedNamespaceDefinition === NamespaceDefinitionType.customformat
            ? "customFormat"
            : watchedNamespaceDefinition
        }.description`}
      />
      <Box py="lg">
        <Text color="grey">
          <FormattedMessage id="connectionForm.modal.destinationNamespace.description" />
        </Text>
      </Box>
      {watchedNamespaceDefinition === NamespaceDefinitionType.source && (
        <Box pb="lg">
          <Text color="grey">
            <FormattedMessage id="connectionForm.modal.destinationNamespace.description.emptySource" />
          </Text>
        </Box>
      )}
      {watchedNamespaceDefinition === NamespaceDefinitionType.customformat && (
        <Box pb="lg">
          <Text color="grey">
            <FormattedMessage id="connectionForm.modal.destinationNamespace.description.emptyCustom" />
          </Text>
        </Box>
      )}
      <ExampleSettingsTable namespaceDefinitionType={watchedNamespaceDefinition} />
      <Box mt="lg">
        <ExternalLink href={links.namespaceLink}>
          <Text size="xs" color="blue">
            <FormattedMessage id="connectionForm.modal.destinationNamespace.learnMore.link" />
          </Text>
        </ExternalLink>
      </Box>
    </>
  );
};
