import {
  DndContext,
  DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useMappingContext } from "./MappingContext";
import { MappingRow } from "./MappingRow";

export const StreamMappingsCard: React.FC<{ streamName: string }> = ({ streamName }) => {
  const { streamsWithMappings, reorderMappings, addMappingForStream } = useMappingContext();
  const mappingsForStream = streamsWithMappings[streamName];
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (active.id && over?.id && active.id !== over.id) {
      const oldIndex = mappingsForStream.findIndex((mapping) => mapping.id === active.id);
      const newIndex = mappingsForStream.findIndex((mapping) => mapping.id === over.id);

      if (oldIndex !== -1 && newIndex !== -1) {
        const updatedOrder = arrayMove(mappingsForStream, oldIndex, newIndex);
        reorderMappings(streamName, updatedOrder);
      }
    }
  };

  return (
    <Card title={streamName}>
      <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
        <SortableContext items={mappingsForStream.map((mapping) => mapping.id)} strategy={verticalListSortingStrategy}>
          <FlexContainer direction="column">
            {mappingsForStream.map((mapping) => (
              <MappingRow key={mapping.id} id={mapping.id} streamName={streamName} />
            ))}
            <Button onClick={() => addMappingForStream(streamName)} variant="secondary" size="sm" width={125}>
              <FormattedMessage id="connections.mappings.addMapping" />
            </Button>
          </FlexContainer>
        </SortableContext>
      </DndContext>
    </Card>
  );
};
