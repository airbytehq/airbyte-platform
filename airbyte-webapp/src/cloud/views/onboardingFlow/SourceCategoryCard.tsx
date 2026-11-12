import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

import styles from "./SourceCategoryCard.module.scss";
import { SourceCheckbox } from "./SourceCheckbox";

interface SourceCategoryCardProps {
  labelId: string;
  icon: IconType;
  definitions: SourceDefinitionRead[];
  picks: string[];
  onToggle: (sourceDefinitionId: string) => void;
}

/** A card opens on its own when it holds a pick, so a returning member sees the saved picks. */
export const SourceCategoryCard: React.FC<SourceCategoryCardProps> = ({
  labelId,
  icon,
  definitions,
  picks,
  onToggle,
}) => {
  const hasPick = definitions.some(({ sourceDefinitionId }) => picks.includes(sourceDefinitionId));

  return (
    <Disclosure as="div" className={styles.category} defaultOpen={hasPick}>
      {({ open }) => (
        <>
          <DisclosureButton className={styles.category__heading}>
            <FlexContainer alignItems="center" gap="md">
              <Icon type={icon} size="lg" color="disabled" />
              <Text size="lg">
                <FormattedMessage id={labelId} />
              </Text>
            </FlexContainer>
            <Icon type={open ? "chevronUp" : "chevronDown"} size="sm" color="disabled" />
          </DisclosureButton>
          <DisclosurePanel className={styles.category__connectors}>
            {definitions.map((definition) => (
              <SourceCheckbox
                key={definition.sourceDefinitionId}
                definition={definition}
                checked={picks.includes(definition.sourceDefinitionId)}
                onToggle={onToggle}
              />
            ))}
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
};
