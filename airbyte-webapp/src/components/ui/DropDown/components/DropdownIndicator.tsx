import React from "react";
import { components, DropdownIndicatorProps } from "react-select";

import { Icon } from "components/ui/Icon";

export const DropdownIndicator: React.FC<DropdownIndicatorProps> = (props) => (
  <components.DropdownIndicator {...props}>
    <Icon type="chevronDown" />
  </components.DropdownIndicator>
);
