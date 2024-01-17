import React from "react";
import { components, DropdownIndicatorProps } from "react-select";

import { Icon } from "components/ui/Icon";

import "./DropdownIndicator.module.scss";

export const DropdownIndicator: React.FC<DropdownIndicatorProps> = (props) => (
  <components.DropdownIndicator {...props} className="fieldsetAwareIndicator">
    <Icon type="chevronDown" />
  </components.DropdownIndicator>
);
