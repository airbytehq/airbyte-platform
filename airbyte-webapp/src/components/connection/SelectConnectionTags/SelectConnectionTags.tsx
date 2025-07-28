import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import classNames from "classnames";
import { useDeferredValue, useState, useMemo, useCallback, useEffect, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { HttpProblem } from "core/api";
import { Tag } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useHeadlessUiOnClose } from "core/utils/useHeadlessUiOnClose";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./SelectConnectionTags.module.scss";

export interface SelectConnectionTagsProps {
  availableTags: Tag[];
  selectedTags: Tag[];
  createTag: (name: string, color: string) => Promise<void>;
  selectTag: (tag: Tag) => void;
  deselectTag: (tag: Tag) => void;
  disabled?: boolean; // intent
  onClose?: () => void;
}

export const THEMED_HEX_OPTIONS = [
  "FBECB1",
  "FEC9BE",
  "FFE5E9",
  "FFD0B2",
  "DDF6F8",
  "CAC7FF",
  "D4DFFC",
  "75DCFF",
  "BDFFC3",
  "DFD5CE",
];

const CONNECTION_TAGS_LIMIT = 10;

const CONNECTION_NAME_LENGTH_LIMIT = 30;

const CreateTagControl: React.FC<{
  createTagLoading: boolean;
  tagName: string;
  handleCreateTag: (tagName: string, color: string) => void;
  color: string;
  disabled?: boolean;
}> = ({ createTagLoading, tagName, handleCreateTag, color, disabled }) => {
  return (
    <button
      className={classNames(styles.selectConnectionTags__tagRow, {
        [styles["selectConnectionTags__tagRow--disabled"]]: disabled,
      })}
      onClick={() => handleCreateTag(tagName, color)}
      disabled={disabled}
      type="button"
    >
      <FlexContainer gap="sm" alignItems="center" justifyContent="flex-start">
        {createTagLoading && <Icon size="xs" type="loading" />}
        <Text>
          <FormattedMessage id="connection.tags.create" />
        </Text>
        <TagBadge color={color} text={tagName} />
      </FlexContainer>
    </button>
  );
};

interface TagRowProps {
  disabled?: boolean;
  handleTagChange: (isChecked: boolean, tag: Tag) => void;
  isSelected: boolean;
  tag: Tag;
}

const TagRow: React.FC<TagRowProps> = ({ disabled, handleTagChange, isSelected, tag }) => {
  return (
    <button
      type="button"
      className={classNames(styles.selectConnectionTags__tagRow, {
        [styles["selectConnectionTags__tagRow--disabled"]]: disabled,
      })}
      onClick={() => handleTagChange(!isSelected, tag)}
      disabled={disabled}
    >
      <FlexContainer gap="md" alignItems="center">
        <CheckBox readOnly disabled={disabled} id={`tag-checkbox-${tag.tagId}`} checked={isSelected} />
        <TagBadge color={tag.color} text={tag.name} />
      </FlexContainer>
    </button>
  );
};

export const SelectConnectionTags: React.FC<SelectConnectionTagsProps> = ({
  availableTags,
  selectedTags,
  createTag,
  selectTag,
  deselectTag,
  disabled,
  onClose,
}) => {
  const [tagsSelectedOnOpen, setTagsSelectedOnOpen] = useState(selectedTags);
  const [query, setQuery] = useState("");
  const [createTagLoading, setCreateTagLoading] = useState(false);
  const notificationService = useNotificationService();
  const formatError = useFormatError();

  const onClosePopover = useCallback(() => {
    setQuery("");
    onClose?.();
    setTagsSelectedOnOpen(selectedTags);
  }, [onClose, selectedTags]);

  const { targetRef } = useHeadlessUiOnClose(onClosePopover);

  const deferredQueryValue = useDeferredValue(query);
  const { formatMessage } = useIntl();

  const handleTagChange = (isNowSelected: boolean, tag: Tag) => {
    if (isNowSelected) {
      selectTag(tag);
    } else {
      deselectTag(tag);
    }
  };

  const color = THEMED_HEX_OPTIONS[availableTags.length % THEMED_HEX_OPTIONS.length]; // todo: should this be more random?

  const tagLimitReached = !(selectedTags.length < CONNECTION_TAGS_LIMIT);

  const filteredTags = useMemo(
    () =>
      availableTags
        .filter((tag) => tag.name.toLocaleLowerCase().includes(deferredQueryValue.trim().toLocaleLowerCase()))
        .sort((a, b) => a.name.localeCompare(b.name)),
    [availableTags, deferredQueryValue]
  );

  // For better UX, the originally selected tags should always be at the top of the list
  const sortedTags = useMemo(() => {
    const selectedTagsSet = new Set(tagsSelectedOnOpen.map((tag) => tag.tagId));

    const topSection: Tag[] = [];
    const bottomSection: Tag[] = [];

    filteredTags.forEach((tag) => (selectedTagsSet.has(tag.tagId) ? topSection.push(tag) : bottomSection.push(tag)));

    return [...topSection, ...bottomSection];
  }, [tagsSelectedOnOpen, filteredTags]);

  const tagAlreadyExists = useMemo(
    () => deferredQueryValue.trim().length > 0 && availableTags.some((tag) => tag.name === deferredQueryValue.trim()),
    [availableTags, deferredQueryValue]
  );

  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });

  const handleCreateTag = async (deferredQueryValue: string, color: string) => {
    if (disabled || createTagLoading || tagAlreadyExists) {
      return;
    }

    setCreateTagLoading(true);
    try {
      await createTag(deferredQueryValue, color);
    } catch (error) {
      notificationService.registerNotification({
        id: "create-tag-error",
        text: HttpProblem.isInstanceOf(error)
          ? formatError(error)
          : formatMessage({ id: "connection.tags.creationError" }),
        type: "error",
      });
    } finally {
      setCreateTagLoading(false);
      setQuery("");
    }
  };

  const handleQueryChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.value.length <= CONNECTION_NAME_LENGTH_LIMIT) {
      setQuery(e.target.value);
    }
  };

  return (
    <Popover ref={targetRef}>
      {() => {
        return (
          <>
            <PopoverButton ref={reference} as="span">
              <Tooltip
                placement="top"
                control={
                  <Button
                    size="sm"
                    variant="secondary"
                    icon={selectedTags.length === 0 ? "plus" : "pencil"}
                    iconSize="sm"
                    className={styles.selectConnectionTags__trigger}
                    data-testid="select-connection-tags-popover"
                  />
                }
              >
                <FormattedMessage id={selectedTags.length === 0 ? "connection.tags.add" : "connection.tags.edit"} />
              </Tooltip>
            </PopoverButton>
            <PopoverPanel
              ref={floating}
              style={{
                position: strategy,
                top: y ?? 0,
                left: x ?? 0,
              }}
              className={styles.selectConnectionTags}
            >
              <SearchOrCreateInput
                disabled={disabled || createTagLoading}
                onChange={handleQueryChange}
                onPressEnter={() => handleCreateTag(deferredQueryValue, color)}
                value={deferredQueryValue}
                hasTags={availableTags.length > 0}
              />
              {deferredQueryValue.trim().length > 0 &&
                !tagAlreadyExists &&
                (!tagLimitReached ? (
                  <CreateTagControl
                    createTagLoading={createTagLoading}
                    handleCreateTag={handleCreateTag}
                    tagName={deferredQueryValue}
                    color={color}
                    disabled={disabled || createTagLoading}
                  />
                ) : (
                  <Tooltip
                    containerClassName={styles.selectConnectionTags__tagRowTooltip}
                    control={
                      <CreateTagControl
                        createTagLoading={createTagLoading}
                        handleCreateTag={handleCreateTag}
                        tagName={deferredQueryValue}
                        color={color}
                        disabled
                      />
                    }
                  >
                    <FormattedMessage id="connection.tags.limitReached" />
                  </Tooltip>
                ))}
              <ul className={styles.selectConnectionTags__tags}>
                {sortedTags.map((tag) => {
                  const isSelected = selectedTags.some((selectedTag) => selectedTag.tagId === tag.tagId);

                  return (
                    <li key={tag.tagId}>
                      {tagLimitReached && !isSelected ? (
                        <Tooltip
                          containerClassName={styles.selectConnectionTags__tagRowTooltip}
                          control={
                            <TagRow disabled handleTagChange={handleTagChange} isSelected={isSelected} tag={tag} />
                          }
                        >
                          <FormattedMessage id="connection.tags.limitReached" />
                        </Tooltip>
                      ) : (
                        <TagRow
                          disabled={disabled}
                          handleTagChange={handleTagChange}
                          isSelected={isSelected}
                          tag={tag}
                        />
                      )}
                    </li>
                  );
                })}
              </ul>
            </PopoverPanel>
          </>
        );
      }}
    </Popover>
  );
};

interface SearchOrCreateInputProps {
  disabled?: boolean;
  hasTags: boolean;
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onPressEnter: () => void;
}

const SearchOrCreateInput: React.FC<SearchOrCreateInputProps> = ({
  disabled,
  onChange,
  onPressEnter,
  value,
  hasTags,
}) => {
  const ref = useRef<HTMLInputElement | null>(null);
  const { formatMessage } = useIntl();

  // This is a workaround for the fact that autoFocus does not work as expected with headless ui's floating popover.
  // The initial rendered position popover causes the page to scroll and potentially close immediately if it gets
  // virtualized away.
  useEffect(() => {
    ref.current?.focus({ preventScroll: true });
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      onPressEnter();
    }
  };

  return (
    <Input
      inline
      ref={ref}
      disabled={disabled}
      value={value}
      onChange={onChange}
      onKeyDown={handleKeyDown}
      containerClassName={styles.selectConnectionTags__input}
      placeholder={formatMessage(
        {
          id: "connection.tags.selectOrCreateTag",
        },
        { hasTags }
      )}
    />
  );
};
