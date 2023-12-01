import path from "path";

import viteYaml from "@modyfi/vite-plugin-yaml";
import basicSsl from "@vitejs/plugin-basic-ssl";
import react from "@vitejs/plugin-react";
import { UserConfig, defineConfig } from "vite";
import checker from "vite-plugin-checker";
import svgrPlugin from "vite-plugin-svgr";
import viteTsconfigPaths from "vite-tsconfig-paths";

import {
  buildInfo,
  compileFormatJsMessages,
  docMiddleware,
  environmentVariables,
  experimentOverwrites,
  preloadTags,
} from "./packages/vite-plugins";

export default defineConfig(() => {
  const config: UserConfig = {
    plugins: [
      environmentVariables(),
      basicSsl(),
      react(),
      buildInfo(),
      compileFormatJsMessages(),
      viteTsconfigPaths({ ignoreConfigErrors: true }),
      viteYaml(),
      svgrPlugin({
        svgrOptions: {
          plugins: ["@svgr/plugin-svgo", "@svgr/plugin-jsx"],
          titleProp: true,
          svgoConfig: {
            plugins: [
              {
                name: "preset-default",
                params: {
                  overrides: {
                    removeViewBox: false,
                    cleanupIds: false,
                    removeUnknownsAndDefaults: {
                      keepRoleAttr: true,
                    },
                  },
                },
              },
            ],
          },
        },
      }),
      checker({
        // Enable checks while building the app (not just in dev mode)
        enableBuild: true,
        overlay: {
          initialIsOpen: false,
          position: "br",
          // Align error popover button with the react-query dev tool button
          badgeStyle: "transform: translate(-75px,-11px); display: var(--show-dev-tools)",
        },
        eslint: { lintCommand: `eslint --max-warnings=0 --ext .js,.ts,.tsx --ignore-path .gitignore .` },
        stylelint: {
          lintCommand: 'stylelint "src/**/*.{css,scss}"',
          // We need to overwrite this during development, since otherwise `files` are wrongly
          // still containing the quotes around them, which they shouldn't
          dev: { overrideConfig: { files: "src/**/*.{css,scss}" } },
        },
        typescript: true,
      }),
      docMiddleware(),
      experimentOverwrites(),
      preloadTags(),
    ],
    // Use `REACT_APP_` as a prefix for environment variables that should be accessible from within FE code.
    envPrefix: ["REACT_APP_"],
    clearScreen: false,
    build: {
      outDir: "build/app",
    },
    server: {
      host: true,
      port: Number(process.env.PORT) || 3000,
      strictPort: true,
      headers: {
        "Content-Security-Policy": "script-src * 'unsafe-inline'; worker-src 'self' blob:;",
      },
    },
    css: {
      devSourcemap: true,
      modules: {
        generateScopedName: "[name]__[local]__[contenthash:6]",
      },
    },
    resolve: {
      alias: {
        // Allow @use "scss/" imports in SASS
        scss: path.resolve(__dirname, "./src/scss"),
      },
    },
  };

  return config;
});
