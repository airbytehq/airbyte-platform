import React, { useState } from "react";

import { Button, ButtonProps } from "components/ui/Button";
import { Text } from "components/ui/Text";

import styles from "./QuickBooksAuthButton.module.scss";
import quickBooksAuthButton from "./quickbooksAuthButton.svg";
import quickBooksAuthButtonHover from "./quickbooksAuthButtonHover.svg";
import { FlexContainer } from "../../../../../../components/ui/Flex";

const QuickBooksAuthButton: React.FC<React.PropsWithChildren<ButtonProps>> = (props) => {
  const [isActive, setIsActive] = useState(false);
  return (
    <FlexContainer direction="column" alignItems="center">
      <Button className={styles.qbAuthButton} {...props}>
        <img
          src={isActive ? quickBooksAuthButtonHover : quickBooksAuthButton}
          alt="Connect to QuickBook"
          onMouseOver={() => setIsActive(true)}
          onMouseOut={() => setIsActive(false)}
          onFocus={() => setIsActive(true)}
          onBlur={() => setIsActive(false)}
        />
      </Button>
      <Text size="sm" color="grey300">
        {props.children}
      </Text>
    </FlexContainer>
  );
};

export default QuickBooksAuthButton;
