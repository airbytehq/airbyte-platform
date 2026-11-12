import React from "react";

import { Icon, IconColor, IconType } from "components/ui/Icon";

export type StatusLevel = "success" | "warning" | "error" | "neutral";

interface LevelIcon {
  type: IconType;
  color: IconColor;
}

const LEVEL_ICON: Record<StatusLevel, LevelIcon> = {
  success: { type: "statusSuccess", color: "success" },
  warning: { type: "statusWarning", color: "warning" },
  error: { type: "statusError", color: "error" },
  neutral: { type: "statusError", color: "disabled" },
};

interface StatusLevelIconProps {
  level: StatusLevel;
}

export const StatusLevelIcon: React.FC<StatusLevelIconProps> = ({ level }) => {
  const { type, color } = LEVEL_ICON[level];

  return <Icon type={type} color={color} size="sm" />;
};
