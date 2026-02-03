import React from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components";
import { FlexContainer } from "components/ui/Flex";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import { SelectConnectionTags } from "area/connection/components/SelectConnectionTags/SelectConnectionTags";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateTag, useTagsList } from "core/api";

export const ConnectionTagsFormField: React.FC = () => {
  const { control, watch, setValue } = useFormContext<FormConnectionFormValues>();
  const workspaceId = useCurrentWorkspaceId();
  const availableTags = useTagsList(workspaceId);
  const { mutateAsync: createTag } = useCreateTag();
  const tags = watch("tags");

  const onCreateTag = async (name: string, color: string) => {
    const newTag = await createTag({ name, color, workspaceId });
    setValue("tags", [...(tags || []), newTag], { shouldDirty: true });
  };

  return (
    <Controller
      name="tags"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor="controlId"
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connection.tags.title" />
                </Text>
              </FlexContainer>
            }
          />
          <FlexContainer gap="sm" alignItems="center">
            {field.value?.map((tag) => <TagBadge color={tag.color} key={tag.tagId} text={tag.name} />)}
            <SelectConnectionTags
              availableTags={availableTags}
              selectedTags={field.value ?? []}
              createTag={onCreateTag}
              selectTag={(selectedTag) => field.onChange([...(field.value || []), selectedTag])}
              deselectTag={(deselectedTag) =>
                field.onChange((field.value || []).filter((tag) => tag.tagId !== deselectedTag.tagId))
              }
            />
          </FlexContainer>
        </FormFieldLayout>
      )}
    />
  );
};
