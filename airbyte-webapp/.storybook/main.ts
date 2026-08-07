import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  framework: "@storybook/react-vite",
  typescript: {
    reactDocgen: "react-docgen-typescript",
  },
  stories: ["../src/**/*.stories.@(ts|tsx)", "../src/**/*.docs.mdx"],
  addons: ["storybook-dark-mode", "@storybook/addon-links", "@storybook/addon-essentials", "@storybook/addon-actions"],
};

export default config;
