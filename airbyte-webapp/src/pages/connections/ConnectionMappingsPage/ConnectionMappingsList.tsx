import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";
import { MAPPING_VALIDATION_ERROR_KEY, useMappingContext } from "./MappingContext";
import { StreamMappingsCard } from "./StreamMappingsCard";

export const ConnectionMappingsList: React.FC = () => {
  const { streamsWithMappings, clear, submitMappings, key } = useMappingContext();
  const { mode } = useConnectionFormService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  const handleValidations = async () => {
    const validations = await Promise.allSettled(
      Object.entries(streamsWithMappings).flatMap(([_streamDescriptorKey, mappers]) =>
        mappers.map((mapper) => mapper.validationCallback())
      )
    );
    if (validations.every((validation) => validation.status === "fulfilled" && validation.value === true)) {
      submitMappings();
    } else {
      registerNotification({
        type: "error",
        text: formatMessage({ id: "connections.mappings.submissionValidationError" }),
        id: MAPPING_VALIDATION_ERROR_KEY,
      });
    }
  };

  return (
    <FlexContainer direction="column">
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
        <Heading as="h3" size="sm">
          <FormattedMessage id="connections.mappings.title" />
        </Heading>
        <FlexContainer>
          <Button variant="secondary" onClick={clear} disabled={mode === "readonly"}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button onClick={handleValidations} disabled={mode === "readonly"}>
            <FormattedMessage id="form.submit" />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <FlexContainer direction="column">
        {Object.entries(streamsWithMappings).map(([streamDescriptorKey, mappers]) => {
          if (!mappers || mappers.length === 0) {
            return null;
          }

          return <StreamMappingsCard key={`${streamDescriptorKey}-${key}`} streamDescriptorKey={streamDescriptorKey} />;
        })}
        <div>
          <AddStreamForMappingComboBox secondary />
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};
