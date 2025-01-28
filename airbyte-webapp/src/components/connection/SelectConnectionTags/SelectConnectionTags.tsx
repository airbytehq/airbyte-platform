import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import classNames from "classnames";
import { useDeferredValue, useState, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { TagBadge } from "components/ui/TagBadge";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { Tag } from "core/api/types/AirbyteClient";

import styles from "./SelectConnectionTags.module.scss";

export interface SelectConnectionTagsProps {
  selectedTags: Tag[];
  availableTags: Tag[];
  createTag: (name: string, color: string) => void;
  selectTag: (id: string) => void;
  deselectTag: (id: string) => void;
  disabled?: boolean; // intent
}

const THEMED_HEX_OPTIONS = [
  "#FBECB1",
  "#FEC9BE",
  "#FFE5E9",
  "#FFD0B2",
  "#DDF6F8",
  "#CAC7FF",
  "#D4DFFC",
  "#75DCFF",
  "#BDFFC3",
  "#DFD5CE",
];

const CONNECTION_TAGS_LIMIT = 10;

const CreateTagControl: React.FC<{
  tagName: string;
  handleCreateTag: (tagName: string, color: string) => void;
  color: string;
  disabled?: boolean;
}> = ({ tagName, handleCreateTag, color, disabled }) => {
  return (
    <button
      className={classNames(styles.selectConnectionTags__tagRow, {
        [styles["selectConnectionTags__tagRow--disabled"]]: disabled,
      })}
      onClick={() => handleCreateTag(tagName, color)}
      disabled={disabled}
    >
      <FlexContainer gap="sm" alignItems="center" justifyContent="flex-start">
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
  handleTagChange: (isChecked: boolean, tagId: string) => void;
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
      onClick={() => handleTagChange(!isSelected, tag.tagId)}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          handleTagChange(isSelected, tag.tagId);
        }
      }}
      disabled={disabled}
    >
      <FlexContainer gap="sm" alignItems="center">
        <CheckBox disabled={disabled} id={`tag-checkbox-${tag.tagId}`} checked={isSelected} />
        <TagBadge color={tag.color} text={tag.name} />
      </FlexContainer>
    </button>
  );
};

interface TriggerButtonProps {
  icon: "plus" | "pencil";
}

/**
 * TODO: refactor our main <Button> so we don't have to reimplement it here
 * https://github.com/airbytehq/airbyte-internal-issues/issues/11481
 */
const TriggerButton: React.FC<TriggerButtonProps> = ({ icon }) => {
  return (
    <button className={styles.selectConnectionTags__trigger}>
      <Icon size="xs" type={icon} />
    </button>
  );
};

export const SelectConnectionTags: React.FC<SelectConnectionTagsProps> = ({
  selectedTags,
  availableTags,
  createTag,
  selectTag,
  deselectTag,
  disabled,
}) => {
  const [query, setQuery] = useState("");

  const deferredQueryValue = useDeferredValue(query);
  const { formatMessage } = useIntl();

  const handleTagChange = (isNowSelected: boolean, tagId: string) => {
    if (isNowSelected) {
      selectTag(tagId);
    } else {
      deselectTag(tagId);
    }
  };

  const color = THEMED_HEX_OPTIONS[availableTags.length % THEMED_HEX_OPTIONS.length]; // todo: should this be more random?

  const tagLimitReached = !(selectedTags.length < CONNECTION_TAGS_LIMIT);

  const filteredTags = useMemo(
    () => availableTags.filter((tag) => tag.name.toLocaleLowerCase().includes(deferredQueryValue.toLocaleLowerCase())),
    [availableTags, deferredQueryValue]
  );

  const tagDoesNotExist = useMemo(
    () => deferredQueryValue.trim().length > 0 && !availableTags.some((tag) => tag.name === deferredQueryValue),
    [availableTags, deferredQueryValue]
  );

  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });

  const handleCreateTag = (deferredQueryValue: string, color: string) => {
    createTag(deferredQueryValue, color);
    setQuery("");
  };

  return (
    <Popover>
      {({ open }) => {
        if (!open) {
          setQuery("");
        }

        return (
          <>
            <PopoverButton ref={reference} as="span">
              <Tooltip placement="top" control={<TriggerButton icon={selectedTags.length === 0 ? "plus" : "pencil"} />}>
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
              <Input
                inline
                value={deferredQueryValue}
                onChange={(e) => setQuery(e.target.value)}
                containerClassName={styles.selectConnectionTags__input}
                placeholder={formatMessage(
                  {
                    id: "connection.tags.selectOrCreateTag",
                  },
                  { hasTags: availableTags.length > 0 }
                )}
              />
              {tagDoesNotExist &&
                (!tagLimitReached ? (
                  <CreateTagControl
                    handleCreateTag={handleCreateTag}
                    tagName={deferredQueryValue}
                    color={color}
                    disabled={disabled}
                  />
                ) : (
                  <Tooltip
                    containerClassName={styles.selectConnectionTags__tagRowTooltip}
                    control={
                      <CreateTagControl
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
              <ul>
                {filteredTags.map((tag) => {
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
