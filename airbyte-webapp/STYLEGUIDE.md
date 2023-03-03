# Frontend Style Guide

This serves as a living document regarding conventions we have agreed upon as a frontend team. In general, the aim of these decisions and discussions is to both (a) increase the readability and consistency of our code and (b) decrease day to day decision-making so we can spend more time writing better code.

## General Code Style and Formatting

- Where possible, we rely on automated systems to maintain consistency in code style
- We use eslint, Prettier, and VSCode settings to automate these choices. The configuration files for these are checked into our repository, so no individual setup should be required beyond ensuring your VSCode settings include:

```
"editor.codeActionsOnSave": {
  "source.fixAll.eslint": true,
}
```

- Don’t use single-character names. Using meaningful name for function parameters is a way of making the code self-documented and we always should do it. Example:
  - .filter(([key, value]) => isDefined(value.default) ✅
  - .filter(([k, v]) => isDefined(v.default) ❌

## Exporting

- Export at declaration, not at the bottom. For example:
  - export const myVar ✅
  - const myVar; export { myVar }; ❌

## Component Props

- Use explicit, verbose naming
  - ie: `interface ConnectionFormProps` not `interface iProps`

## Testing

- Test files should be store alongside the files/features they are testing
- Use the prop `data-testid` instead of `data-id`

## Types

- For component props, prefer type unions over enums:
  - `type SomeType = “some” | “type”;` ✅
  - `enum SomeEnum = { SOME: “some”, TYPE: “type” };` ❌
  - Exceptions may include:
    - Generated using enums from the API
    - When the value on an enum is cleaner than the string
      - In this case use `const enum` instead

## Styling

### Color variables cannot be used inside of rgba() functions

Our SCSS color variables compile to `rgb(X, Y, Z)`, which is an invalid value in the CSS `rgba()` function. A custom stylelint rule, `airbyte/no-color-variables-in-rgba`, enforces this rule.

❌ Incorrect

```scss
@use "scss/colors";

.myClass {
  background-color: rgba(colors.$blue-400, 50%);
}
```

✅ Correct - define a color variable with transparency and use it directly

```scss
@use "scss/colors";

.myClass {
  background-color: colors.$blue-transparent;
}
```

> Historical context: previously there was some usage of color variables inside `rgba()` functions. In these cases, _SASS_ was actually compiling this into CSS for us, because it knew to convert `rgba(rgb(255, 0, 0), 50%)` into `rgb(255, 0, 0, 50%)`. Now that we use CSS Custom Properties for colors, SASS cannot know the value of the color variable at build time. So it outputs `rgba(var(--blue), 50%)` which will not work as expected.

## Folder Structure

> The Airbyte team is currently restructuring how folders are organized in the codebase. This section describes the current folder structure and may be subject to change.

* components - All React components except for page components. See section below for details
* config - App-wide configuration
* core - General modules used by the app
* hooks - Shared hooks
* locales - i18n files.
* packages - Folder deprecated. Sub-folders will be moved out in the future
* pages - React components that represents the app routes and pages. See section below
* scss - Global SCSS files
* services -
* test-utils - Unit testing utilities
* types - Shared TypeScript types and external module type definitions
* utils - Shared utilities
* views - Folder deprecated. Includes React components that will be moved to `components/` in the future

## React Components

> The Airbyte team is currently restructuring how components are organized in the codebase. Components currently in `src/views/` are being be migrated to `src/components/`. `src/components` is currently being reorganized. The new structure is described below and it's expected that new code follows the new structure.

Most React components are in `src/components` but components that represent pages on the site are in `src/pages`. 

### Components Structure

The `src/components` folder is divided into sub-folders for each domain in the app such as `connection`, `source`, and `destination`. Core UI components (such as Buttons, tables, etc.) are all located in `src/components/ui`. Components that are shared across different domains but may not be part of the UI library are in `src/components/common`. Sub-folders must be written in `camelCase`.

Within each sub-folder, there are folders for each major component. These folders are written in `PascalCase`, the same way a React component would be named. Within these folders there are a few files:

* `index.ts` - Used to export the main component and supporting functions or types to the app.
* `{MainComponentName}.tsx` - The main component, also exported through `index.ts`
* `{MainComponentName}.test.tsx` - The test file
* `{MainComponentName}.module.scss` - The main component's style file
* Files for additional components that support the main Component directly, using the same naming conventions as above.
* `types.ts` (optional) - Types shared between different sub-components
* `utils.ts` (optional) - Any utilities that support the components

When using supporting components, the folder could become rather full of them. In those cases components should be split into their own folders parallel to the main component, especially when the supporting component also needs to be broken down into multiple sub components.

Here's a hypothetical example: The app has a streams panel with a lot of sub-components including a streams table. Instead of placing all the components under `src/components/connection/StreamsPanel`, the table should be broken out into its own sub-folder as a child of the `connection` folder.

❌ Incorrect - While StreamsTable is only used in the StreamsPanel, it also has its own sub-components.

```
src/
  components/
    connection/
      StreamsPanel/
        index.ts
        StreamsPanel.tsx
        StreamsPanelHeader.tsx
        StreamsPanelContent.tsx
        StreamsTable.tsx
        StreamsTableBody.tsx
        StreamsTableCell.tsx
        StreamsTableHeader.tsx
        StreamsTableRow.tsx
```

✅ Correct - StreamsTable is separated into its own folder alongside StreamsPanel

```
src/
  components/
    connection/
      StreamsPanel/
        index.ts
        StreamsPanel.tsx
        StreamsPanelHeader.tsx
        StreamsPanelContent.tsx
      StreamsTable/
        index.ts
        StreamsTable.tsx
        StreamsTableBody.tsx
        StreamsTableCell.tsx
        StreamsTableHeader.tsx
        StreamsTableRow.tsx
```

### Pages Structure

> The Airbyte team is currently restructuring how pages are organized in the codebase. This describes the ideal structure for the `pages/` folder.

React components that represent pages and routes are split from the rest of the components in order to provide a clear way to identify the individual pages that are available in the app.

Each sub-folder in `pages/` corresponds to the domain path in the url. For example. `/workspaces/{workspaceId}/connections/*` pages are available in `pages/connections/`.

Pages are not nested within sub-folders. Instead, all pages are alongside each other and the nesting is configured in the routes file.

The page React component is intended to put together a series of more heavy-hitting components. The components that support the page are part of the `components/` folder. Smaller components that support the page component can be created in the page's folder.

Here's an example using the connections pages:

```
components/
  connection/
    ConnectionsTable/ - A table listing all connections for a given workspace
pages
  connections/
    AllConnectionsPage/ - The connection overview page. Renders a ConnectionsTable
    ConnectionPage/ - The individual connection page
    ConnectionReplicationPage/ - A sub-page of the ConnectionPage that renders the replication "tab"
    ConnectionRoutes.tsx - Defines entire route structure for connections
routePaths.tsx - Global routing constants
routes.tsx - Global routing file
```
