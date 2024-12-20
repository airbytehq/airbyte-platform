import { FlexContainer } from "components/ui/Flex";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";
import { useMappingContext } from "./MappingContext";
import { StreamMappingsCard } from "./StreamMappingsCard";

export const ConnectionMappingsList: React.FC = () => {
  const { streamsWithMappings, key } = useMappingContext();

  return (
    <FlexContainer direction="column">
      {Object.entries(streamsWithMappings)
        .filter(([_streamDescriptorKey, mappers]) => mappers.length > 0)
        .map(([streamDescriptorKey, mappers]) => {
          if (!mappers || mappers.length === 0) {
            return null;
          }
          return <StreamMappingsCard key={`${streamDescriptorKey}-${key}`} streamDescriptorKey={streamDescriptorKey} />;
        })}
      <div>
        <AddStreamForMappingComboBox secondary />
      </div>
    </FlexContainer>
  );
};
