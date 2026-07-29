import { addons } from "@storybook/manager-api";
import theme from "./theme";

addons.setConfig({
  panelPosition: "bottom",
  theme,
  enableShortcuts: false,
});
