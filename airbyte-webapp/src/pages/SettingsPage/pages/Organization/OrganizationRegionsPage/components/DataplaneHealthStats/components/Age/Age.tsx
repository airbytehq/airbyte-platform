import React from "react";
import { FormattedMessage } from "react-intl";

import { AgeMessage } from "../../types";

interface AgeProps {
  age: AgeMessage;
}

export const Age: React.FC<AgeProps> = ({ age }) => <FormattedMessage id={age.messageId} values={age.values} />;
