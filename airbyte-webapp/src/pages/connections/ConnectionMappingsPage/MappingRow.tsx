import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { PropsWithChildren, useMemo } from "react";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { StreamMapperType } from "core/api/types/AirbyteClient";

import { EncryptionRow } from "./EncryptionRow";
import { FieldRenamingRow } from "./FieldRenamingRow";
import { HashFieldRow } from "./HashFieldRow";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { RowFilteringMapperForm } from "./RowFilteringMapperForm";

export const SupportedMappingTypes = [
  StreamMapperType.hashing,
  StreamMapperType["field-renaming"],
  StreamMapperType["row-filtering"],
  StreamMapperType.encryption,
] as const;

export const MappingRow: React.FC<{
  streamName: string;
  id: string;
}> = ({ streamName, id }) => {
  const { removeMapping, streamsWithMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamName].find((m) => m.mapperConfiguration.id === id);

  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  const RowContent = useMemo(() => {
    if (mapping?.type === StreamMapperType.hashing) {
      return <HashFieldRow streamName={streamName} mappingId={mapping.mapperConfiguration.id} />;
    }
    if (mapping?.type === StreamMapperType["field-renaming"]) {
      return <FieldRenamingRow streamName={streamName} mappingId={mapping.mapperConfiguration.id} />;
    }
    if (mapping?.type === StreamMapperType["row-filtering"]) {
      return <RowFilteringMapperForm streamName={streamName} mappingId={mapping.mapperConfiguration.id} />;
    }
    if (mapping?.type === StreamMapperType.encryption) {
      return <EncryptionRow streamName={streamName} mappingId={mapping.mapperConfiguration.id} />;
    }

    return null;
  }, [mapping, streamName]);

  if (!RowContent || !mapping) {
    return null;
  }

  return (
    <div ref={setNodeRef} style={style}>
      <FlexContainer direction="row" alignItems="center" justifyContent="space-between" className={styles.row}>
        <FlexContainer direction="row" alignItems="center">
          <Button type="button" variant="clear" {...listeners} {...attributes}>
            <Icon color="disabled" type="drag" />
          </Button>
          {RowContent}
        </FlexContainer>
        <Button
          key={`remove-${id}`}
          variant="clear"
          type="button"
          onClick={() => removeMapping(streamName, mapping.mapperConfiguration.id)}
        >
          <Icon color="disabled" type="trash" />
        </Button>
      </FlexContainer>
    </div>
  );
};

export const MappingRowContent: React.FC<PropsWithChildren> = ({ children }) => {
  return (
    <FlexContainer direction="row" alignItems="center" className={styles.rowContent}>
      {children}
    </FlexContainer>
  );
};

export const MappingRowInputWrapper: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.input}>{children}</div>;
};
