import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";
import { useMappingContext } from "./MappingContext";
import { StreamMappingsCard } from "./StreamMappingsCard";

export const ConnectionMappingsList: React.FC = () => {
  const { streamsWithMappings, clear, submitMappings } = useMappingContext();
  const { mode } = useConnectionFormService();

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
          <Button onClick={submitMappings} disabled={mode === "readonly"}>
            <FormattedMessage id="form.submit" />
          </Button>
        </FlexContainer>
      </FlexContainer>
      <FlexContainer direction="column">
        {Object.entries(streamsWithMappings).map(([streamName, mappers]) => {
          if (!mappers || mappers.length === 0) {
            return null;
          }

          return <StreamMappingsCard key={streamName} streamName={streamName} />;
        })}
        <div>
          <AddStreamForMappingComboBox secondary />
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};
