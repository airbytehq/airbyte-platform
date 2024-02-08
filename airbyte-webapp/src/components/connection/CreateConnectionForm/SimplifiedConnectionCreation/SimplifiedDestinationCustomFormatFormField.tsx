import { useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { DestinationNamespaceFormValues } from "components/connection/DestinationNamespaceModal";
import { FormControl } from "components/forms";

export const SimplifiedDestinationCustomFormatFormField: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch, trigger } = useFormContext<DestinationNamespaceFormValues>();
  const watchedNamespaceDefinition = watch("namespaceDefinition");

  useEffect(() => {
    trigger("namespaceFormat", { shouldFocus: true });
  }, [trigger, watchedNamespaceDefinition]);

  return (
    <FormControl
      name="namespaceFormat"
      fieldType="input"
      type="text"
      placeholder={formatMessage({
        id: "connectionForm.modal.destinationNamespace.input.placeholder",
      })}
      data-testid="namespace-definition-custom-format-input"
    />
  );
};
