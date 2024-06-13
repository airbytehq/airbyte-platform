import classNames from "classnames";
import React, { useEffect, useRef, useState } from "react";
import { useToggle } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./ResetStreamDetails.module.scss";

interface ResetStreamsDetailsProps {
  names?: string[];
}

export const ResetStreamsDetails: React.FC<ResetStreamsDetailsProps> = ({ names = [] }) => {
  const textRef = useRef<HTMLParagraphElement>(null);
  const [isExpanded, setIsExpanded] = useToggle(false);
  const [isExpandButtonVisible, setIsExpandButtonVisible] = useState<boolean>(false);
  const onIconClick = () => setIsExpanded();
  useEffect(() => {
    const textCurrent = textRef.current;
    // detect text overflow
    if (textCurrent) {
      setIsExpandButtonVisible(textCurrent.scrollWidth > textCurrent.clientWidth);
    }
  }, []);

  return (
    <FlexContainer direction="column" justifyContent="center" alignItems="center" gap="none">
      <Text ref={textRef} size="sm" className={classNames(styles.textContainer, { [styles.expanded]: isExpanded })}>
        {names.map((name, idx) => (
          <span key={idx} className={styles.text}>
            {name}
          </span>
        ))}
      </Text>
      {isExpandButtonVisible && (
        <Button size="xs" onClick={onIconClick} variant="clear" className={styles.expandBtn}>
          <Icon type={isExpanded ? "chevronUp" : "chevronDown"} />
        </Button>
      )}
    </FlexContainer>
  );
};
