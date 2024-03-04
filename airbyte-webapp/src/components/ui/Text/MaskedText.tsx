import { PropsWithChildren } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { TextProps, Text } from "./Text";
import { Button } from "../Button";
import { FlexContainer } from "../Flex";
import { Icon } from "../Icon";

export const MaskedText: React.FC<PropsWithChildren<TextProps>> = ({ children, ...props }) => {
  const [maskText, toggleMaskText] = useToggle(true);

  return (
    <FlexContainer alignItems="center" gap="none">
      <Text color="grey400" {...props}>
        {maskText ? <FormattedMessage id="general.maskedString" /> : children}
      </Text>
      <Button variant="clear" icon={<Icon type={maskText ? "eye" : "eyeSlash"} />} onClick={toggleMaskText} size="xs" />
    </FlexContainer>
  );
};
