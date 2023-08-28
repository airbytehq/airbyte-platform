import type { StorybookConfig } from "@storybook/react-vite";
import remarkGfm from "remark-gfm";

const config: StorybookConfig = {
  framework: "@storybook/react-vite",
  stories: ["../src/**/*.stories.@(ts|tsx)", "../src/**/*.docs.mdx"],
  addons: [
    "storybook-dark-mode",
    "@storybook/addon-links",
    "@storybook/addon-essentials",
    {
      name: "@storybook/addon-actions",
      options: {
        mdxPluginOptions: {
          mdxCompileOptions: {
            remarkPlugins: [remarkGfm],
          },
        },
      },
    },
  ],
};

export default config;
