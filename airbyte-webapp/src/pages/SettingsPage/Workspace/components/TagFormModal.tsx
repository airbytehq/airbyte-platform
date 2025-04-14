import { useRef } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";
import { z } from "zod";

import { THEMED_HEX_OPTIONS } from "components/connection/SelectConnectionTags/SelectConnectionTags";
import { Form } from "components/forms/Form";
import { FormControl } from "components/forms/FormControl";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { TagBadge } from "components/ui/TagBadge";

import { useCreateTag, useUpdateTag } from "core/api";
import { Tag } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";

import styles from "./TagFormModal.module.scss";

/**
 * Get a random tag color from the themed hex options.
 * @example getRandomTagColor() -> "FBECB1"
 */
const getRandomTagColor = () => {
  const randomIndex = Math.floor(Math.random() * THEMED_HEX_OPTIONS.length);
  return THEMED_HEX_OPTIONS[randomIndex];
};

const tagFormSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error").max(30, "settings.workspace.tags.tagForm.validation.maxLength"),
  color: z
    .string()
    .length(7, "settings.workspace.tags.tagForm.validation.invalidHexColor")
    .regex(/^#([A-Fa-f0-9]{6})$/, "settings.workspace.tags.tagForm.validation.invalidHexColor"),
});

type TagFormValues = z.infer<typeof tagFormSchema>;

interface TagFormModalProps {
  tag?: Tag;
  onCancel: () => void;
  onComplete: () => void;
}

const InnerTagForm: React.FC<Omit<TagFormModalProps, "onComplete">> = ({ onCancel }) => {
  const { formatMessage } = useIntl();
  const { watch, setValue } = useFormContext<TagFormValues>();
  const name = watch("name");
  const color = watch("color");
  const colorInputRef = useRef<HTMLInputElement>(null);

  return (
    <>
      <ModalBody>
        <FlexContainer
          justifyContent="center"
          alignItems="center"
          direction="column"
          className={styles.previewContainer}
        >
          {name.length > 0 && <TagBadge text={name} color={color.slice(1)} />}
        </FlexContainer>
        <FormControl<TagFormValues>
          name="name"
          fieldType="input"
          label={formatMessage({ id: "settings.workspace.tags.tagForm.name" })}
        />
        <div className={styles.hexColorInputContainer}>
          <FormControl<TagFormValues>
            name="color"
            fieldType="input"
            label={formatMessage({ id: "settings.workspace.tags.tagForm.color" })}
            footer={formatMessage({ id: "settings.workspace.tags.tagForm.colorDescription" })}
            className={styles.hexColorInput}
          />
          <input
            type="color"
            className={styles.colorInput}
            value={color}
            onChange={(e) => setValue("color", e.target.value, { shouldDirty: true, shouldValidate: true })}
            ref={colorInputRef}
          />
          <div
            className={styles.colorPickerCircle}
            style={{ "--preview-color": color } as React.CSSProperties}
            role="button"
            onClick={() => colorInputRef.current?.click()}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                colorInputRef.current?.click();
              }
            }}
            tabIndex={0}
          />
        </div>
      </ModalBody>
      <ModalFooter>
        <FormSubmissionButtons
          allowNonDirtyCancel
          onCancelClickCallback={onCancel}
          justify="flex-start"
          submitKey="form.saveChanges"
        />
      </ModalFooter>
    </>
  );
};

export const TagFormModal: React.FC<TagFormModalProps> = ({ tag, onCancel, onComplete }) => {
  const { formatMessage } = useIntl();
  const formatError = useFormatError();
  const { workspaceId } = useCurrentWorkspace();
  const { mutateAsync: createTag } = useCreateTag();
  const { mutateAsync: updateTag } = useUpdateTag();
  const { registerNotification } = useNotificationService();

  const onSubmit = async (values: TagFormValues) => {
    const formattedColor = values.color.slice(1).toUpperCase().trim(); // Remove the #, convert to uppercase and trim
    if (tag) {
      await updateTag({ workspaceId, tagId: tag.tagId, ...values, color: formattedColor });
    } else {
      await createTag({ workspaceId, ...values, color: formattedColor });
    }
  };

  const onSuccess = () => {
    onComplete();
    registerNotification({
      id: "tag_modify_success",
      text: formatMessage({
        id: tag ? "settings.workspace.tags.tagUpdateSuccess" : "settings.workspace.tags.tagCreateSuccess",
      }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: TagFormValues) => {
    trackError(e, { name });

    registerNotification({
      id: "tag_modify_error",
      text: `${formatMessage({
        id: tag ? "settings.workspace.tags.tagUpdateError" : "settings.workspace.tags.tagCreateError",
      })}: ${formatError(e)}`,
      type: "error",
    });
  };

  return (
    <Form<TagFormValues>
      zodSchema={tagFormSchema}
      defaultValues={{
        name: tag?.name ?? "",
        color: tag ? `#${tag.color}` : `#${getRandomTagColor()}`,
      }}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      trackDirtyChanges
    >
      <InnerTagForm onCancel={onCancel} />
    </Form>
  );
};
