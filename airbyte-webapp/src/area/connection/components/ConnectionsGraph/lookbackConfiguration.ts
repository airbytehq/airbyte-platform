import { ManipulateType, UnitType } from "dayjs";
import { FormatDateOptions } from "react-intl";

export interface LookbackConfiguration {
  // windowLength must an integer. A float will break the date math.
  windowLength: number;
  totalLength: number;
  windowsPerTick: number;
  unit: ManipulateType & UnitType;
  tickDateFormatOptions: FormatDateOptions;
}

export type LookbackWindow = "6h" | "24h" | "7d" | "30d";

export const lookbackConfigs: Record<LookbackWindow, LookbackConfiguration> = {
  "6h": {
    unit: "minute",
    windowLength: 15,
    totalLength: 60 * 6,
    windowsPerTick: 4,
    tickDateFormatOptions: { hour: "numeric", minute: "2-digit" },
  },
  "24h": {
    unit: "hour",
    windowLength: 1,
    totalLength: 24,
    windowsPerTick: 1,
    tickDateFormatOptions: { hour: "numeric", minute: "2-digit" },
  },
  "7d": {
    unit: "day",
    windowLength: 1,
    totalLength: 7,
    windowsPerTick: 1,
    tickDateFormatOptions: { day: "numeric", month: "short", weekday: "short" },
  },
  "30d": {
    unit: "day",
    windowLength: 1,
    totalLength: 30,
    windowsPerTick: 7,
    tickDateFormatOptions: { day: "numeric", month: "short" },
  },
};
