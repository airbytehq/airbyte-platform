import classNames from "classnames";
import React, { createContext, useContext } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./SectionProgress.module.scss";

interface SectionProgressValue {
  activeSectionIndex: number;
  sectionCount: number;
}

const SectionProgressContext = createContext<SectionProgressValue | undefined>(undefined);

export const SectionProgressProvider = SectionProgressContext.Provider;

/** Returns undefined outside a form with sections. */
export const useSectionProgress = () => useContext(SectionProgressContext);

export const SectionProgress: React.FC<SectionProgressValue> = ({ activeSectionIndex, sectionCount }) => (
  <FlexContainer alignItems="center" gap="md" data-testid="connector-form-section-progress">
    <Text color="grey" role="status">
      <FormattedMessage
        id="connectorForm.sections.progress"
        values={{ current: activeSectionIndex + 1, total: sectionCount }}
      />
    </Text>
    <FlexContainer gap="xs" aria-hidden>
      {Array.from({ length: sectionCount }, (_, sectionIndex) => (
        <span
          key={sectionIndex}
          className={classNames(styles.segment, { [styles["segment--filled"]]: sectionIndex <= activeSectionIndex })}
        />
      ))}
    </FlexContainer>
  </FlexContainer>
);
