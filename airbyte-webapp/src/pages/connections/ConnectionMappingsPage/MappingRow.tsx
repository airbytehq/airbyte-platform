import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { PropsWithChildren, useMemo } from "react";
import { FieldValues, Path, get, useFormContext, useFormState } from "react-hook-form";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";

import { EncryptionRow } from "./EncryptionRow";
import { FieldRenamingRow } from "./FieldRenamingRow";
import { HashFieldRow } from "./HashFieldRow";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { RowFilteringMapperForm } from "./RowFilteringMapperForm";
import { isEncryptionMapping, isFieldRenamingMapping, isHashingMapping, isRowFilteringMapping } from "./typeHelpers";

export const MappingRow: React.FC<{
  streamDescriptorKey: string;
  id: string;
}> = ({ streamDescriptorKey, id }) => {
  const { removeMapping, streamsWithMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamDescriptorKey].find((m) => m.id === id);

  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  const RowContent = useMemo(() => {
    if (!mapping) {
      return null;
    }

    if (isHashingMapping(mapping)) {
      return <HashFieldRow streamDescriptorKey={streamDescriptorKey} mapping={mapping} />;
    }
    if (isFieldRenamingMapping(mapping)) {
      return <FieldRenamingRow streamDescriptorKey={streamDescriptorKey} mapping={mapping} />;
    }
    if (isRowFilteringMapping(mapping)) {
      return <RowFilteringMapperForm streamDescriptorKey={streamDescriptorKey} mapping={mapping} />;
    }
    if (isEncryptionMapping(mapping)) {
      return <EncryptionRow streamDescriptorKey={streamDescriptorKey} mapping={mapping} />;
    }

    return null;
  }, [mapping, streamDescriptorKey]);

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
          onClick={() => removeMapping(streamDescriptorKey, mapping.id)}
        >
          <Icon color="disabled" type="trash" />
        </Button>
      </FlexContainer>
    </div>
  );
};

export const MappingRowContent: React.FC<PropsWithChildren> = ({ children }) => {
  return (
    <FlexContainer alignItems="flex-start" className={styles.row__content}>
      {children}
    </FlexContainer>
  );
};

export const MappingRowItem: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.row__contentItem}>{children}</div>;
};

interface MappingTypeListBoxProps<TFormValues> {
  name: Path<TFormValues>;
  placeholder: string;
  testId?: string;
}

export const MappingFormTextInput = <TFormValues extends FieldValues>({
  name,
  placeholder,
  testId,
}: MappingTypeListBoxProps<TFormValues>) => {
  const { register } = useFormContext();
  const { errors } = useFormState<TFormValues>({ name });
  const error = get(errors, name);

  return <Input error={!!error} placeholder={placeholder} {...register(name)} data-testid={testId} />;
};
