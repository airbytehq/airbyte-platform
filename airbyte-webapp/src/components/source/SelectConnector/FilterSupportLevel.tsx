import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import { SupportLevel } from "core/api/types/AirbyteClient";

import styles from "./FilterSupportLevel.module.scss";

interface FilterSupportLevelProps {
  selectedSupportLevels: SupportLevel[];
  onUpdateSelectedSupportLevels: (newSupportLevels: SupportLevel[]) => void;
  availableSupportLevels: SupportLevel[];
}

export const FilterSupportLevel: React.FC<FilterSupportLevelProps> = ({
  selectedSupportLevels,
  onUpdateSelectedSupportLevels,
  availableSupportLevels,
}) => {
  const handleChange = (stage: SupportLevel, isSelected: boolean) => {
    if (isSelected) {
      onUpdateSelectedSupportLevels([...selectedSupportLevels, stage]);
    } else {
      onUpdateSelectedSupportLevels(selectedSupportLevels.filter((s) => s !== stage));
    }
  };

  // It's possible that there are no custom connectors, so that filter is hidden. But that filter might
  // still be technically selected, because we cache the user's selection in local storage.
  // In that case we want to know how many of the filters that _are_ visible have been selected.
  const numberOfVisiblySelectedSupportLevels = useMemo(() => {
    return selectedSupportLevels.filter((stage) => availableSupportLevels.includes(stage)).length;
  }, [selectedSupportLevels, availableSupportLevels]);

  return (
    <FlexContainer gap="lg" alignItems="center">
      <Text>
        <FormattedMessage id="connector.filterBy" />
      </Text>
      {availableSupportLevels.flatMap((level, index) => {
        const id = `filter-support-level-${level}`;
        const isChecked = selectedSupportLevels.includes(level);
        return [
          // separator inbetween each filter
          ...(index !== 0
            ? [
                <Text key={`${id}-separator`} color="grey300">
                  |
                </Text>,
              ]
            : []),
          // rule doesn't understand SupportLevelBadge renders text
          // eslint-disable-next-line jsx-a11y/label-has-associated-control
          <label htmlFor={id} className={styles.checkboxLabel} key={id}>
            <FlexContainer alignItems="center" gap="sm">
              <CheckBox
                checkboxSize="sm"
                checked={isChecked}
                disabled={isChecked && numberOfVisiblySelectedSupportLevels <= 1}
                onChange={() => handleChange(level, !isChecked)}
                id={id}
              />
              <SupportLevelBadge supportLevel={level} custom={level === SupportLevel.none} />
            </FlexContainer>
          </label>,
        ];
      })}
    </FlexContainer>
  );
};
