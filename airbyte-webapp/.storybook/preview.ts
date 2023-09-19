import { withProviders } from "./withProvider";

import "../public/index.css";
import "../src/scss/global.scss";
import "../src/dayjs-setup";

export const parameters = {
  darkMode: {
    stylePreview: true,
    darkClass: ["airbyteThemeDark"],
    lightClass: ["airbyteThemeLight"],
  },
  backgrounds: {
    disable: true,
  },
};
export const decorators = [withProviders];
