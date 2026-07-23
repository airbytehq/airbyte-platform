# Frontend Style Guide

This serves as a living document regarding conventions we have agreed upon as a frontend team. In general, the aim of these decisions and discussions is to both (a) increase the readability and consistency of our code and (b) decrease day to day decision-making so we can spend more time writing better code.

## General Code Style and Formatting

 Where possible, we rely on automated systems (ESLint, prettier, stylelint) to maintain consistency in code style. The configuration files including VSCode workspace settings and recommended extensions are checked into our repository, so no individual setup should be required - just open the `airbyte-webapp` folder in your VSCode.

- Don't use single-character names. Using meaningful name for function parameters is a way of making the code self-documented and we always should do it. Example:
  - `.filter(([key, value]) => isDefined(value.default))` ✅
  - `.filter(([k, v]) => isDefined(v.default))` ❌

## ESLint

When disabling ESLint rules, always leave a comment explaining why the rule was disabled. This helps other developers understand the reasoning and prevents cargo-cult copying of disabled rules.

✅ Correct

```typescript
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- Legacy API returns untyped data
const data: any = await legacyApiCall();
```

❌ Incorrect

```typescript
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const data: any = await legacyApiCall();
```

## Spacing and Layout

The following recommendations are all in service to two broader principles:

- components should be reusable in as many different contexts as possible without changing their "internal" CSS
- the impact on layout and spacing of adding any component to an existing page should be predictable

If naively following the recommendations ever undermines those principles, by all means
make an exception; but this should only happen rarely, if ever.

### container elements are responsible for the spacing of their contents, or: a component's responsibility ends at its border

This keeps context-specific spacing definitions as local as possible, and promotes reusing
elements in new contexts without the need to override or add special cases to pre-existing
style rules.

- for outer gaps, prefer a `padding` rule on the container to `margin` rules on child elements
- for inner gaps, prefer flexbox gaps to `margin` rules on child elements

### if you must use margins for spacing between content components, prefer `margin-top` to `margin-bottom`

Sometimes it's impractical to rewrite a whole pre-existing page layout just to make a
drive-by change or addition. In these cases, prefer `margin-top` for spacing: it's a
better fit for the cascading nature of web content and styling.

CSS style rules are applied to elements in source order; once an element has been styled,
there's nothing following elements can do about it. Preceding elements, however, have the
[adjacent sibling combinator (`+`)](https://developer.mozilla.org/en-US/docs/Web/CSS/Adjacent_sibling_combinator) as an
escape hatch to select and style an immediately following element. This is, emphatically,
a hack, and should not be used anywhere it can be avoided; but flexibility via a hack is
preferable to no flexibility at all.

### Naming conventions

A single `.module.scss` file should correspond to a single `.tsx` component file. Within the `.module.scss` file, styles should follow a [BEM](https://getbem.com/introduction/) naming scheme:

```scss
// MyComponent.module.scss
@use "scss/colors";
@use "scss/variables";

// block
.myComponent {
  background-color: colors.$blue-400;
  // modifier
  &--highlighted {
    border: variables.$border-thick solid colors.$green-400;
  }
  // element
  &__element {
    background-color: colors.$grey-300;
    // modifier
    &--error {
      color: colors.$red;
    }
  }
}
```

This helps us keep structure in the source files. This is how these styles can be referenced in React components:

```jsx
import styles from "./MyComponent.module.scss";
import classNames from "classnames";
const MyComponent = ({ isHighlighted, hasError }) => {
  return (
    <div className={classNames(styles.myComponent, { [styles["myComponent--highlighted"]]: isHighlighted })}>
      <p>
        Hello, world!
      </p>
      {hasError && <span className={classNames(styles.myComponent__element, [styles['myComponent__element--error']: hasError])}>Uh oh, error!</span>}
    </div>
  );
};
```

Alternatively, the `classNames()` call can be extracted, which keeps the JSX cleaner:

```jsx
import styles from "./MyComponent.module.scss";
import classNames from "classnames";
const MyComponent = ({ isHighlighted, hasError }) => {
  const componentClasses = classNames(styles.myComponent, {
    [styles["myComponent--highlighted"]]: isHighlighted,
  });

  const elementClasses = classNames(styles.myComponent__element, {
    [styles["myComponent__element--error"]]: hasError,
  });

  return (
    <div className={componentClasses}>
      <p>Hello, world!</p>
      {hasError && <span className={elementClasses}>Uh oh, error!</span>}
    </div>
  );
};
```

## Exporting

- Export at declaration, not at the bottom. For example:
  - `export const myVar` ✅
  - `const myVar; export { myVar };` ❌

## Component Props

- Use explicit, verbose naming
  - ie: `interface ConnectionFormProps` not `interface iProps`

## Testing

- Test files should be store alongside the files/features they are testing
- Use the prop `data-testid` instead of `data-id`

## Launch Darkly Experiments

When developing a new component version to replace an existing one using Launch Darkly experiments (not feature flags), use the "Next" prefix to distinguish between versions. Place the experimental component in the same folder as the original component.

✅ Correct

```typescript
// In src/components/ui/Button/
import { useExperiment } from "core/services/Experiment";
import { Button } from "./Button";
import { NextButton } from "./NextButton";

const MyComponent = () => {
  const isNewButtonEnabled = useExperiment("button.newDesign");

  return isNewButtonEnabled ? <NextButton /> : <Button />;
};
```

This convention makes it easy to:
- Identify experimental components at a glance
- Keep related versions together in the codebase
- Remove the old version once the experiment succeeds

## TypeScript Best Practices

### Types vs Enums

- For component props, prefer type unions over enums:
  - `type SomeType = "some" | "type";` ✅
  - `enum SomeEnum = { SOME: "some", TYPE: "type" };` ❌
  - Exceptions may include:
    - Generated using enums from the API
    - When the value on an enum is cleaner than the string
      - In this case use `const enum` instead

### Data Transformation Patterns

Chain data transformations instead of storing intermediate variables. This makes the code more concise and easier to read.

❌ Incorrect

```typescript
const filteredData = data.filter((item) => item.id > 0);
const mappedData = filteredData.map((item) => item.name);
return mappedData;
```

✅ Correct

```typescript
return data
  .filter((item) => item.id > 0)
  .map((item) => item.name);
```

### Pure Functions

Prefer pure functions that don't have side effects. Pure functions are easier to test, debug, and reason about.

❌ Incorrect

```typescript
let counter = 0;

function incrementCounter(): number {
  counter++; // Side effect: modifies external variable
  return counter;
}
```

✅ Correct

```typescript
const incrementCounter = (counter: number) => counter + 1;
```

### Extracting Reusable Callbacks

If a callback function is reusable across multiple places, extract it to a separate utility function. Balance this with keeping simple, one-off callbacks inline.

❌ Incorrect - extracting when not needed

```typescript
const filterById = (item: Item) => item.id > 0;
const mapToName = (item: Item) => item.name;

// Used only once
return data.filter(filterById).map(mapToName);
```

✅ Correct - extract when reused

```typescript
// In src/area/connection/utils/connectionHelpers.ts
export const isActiveConnection = (connection: Connection) =>
  connection.status === "active";

// Used in multiple components
const activeConnections = connections.filter(isActiveConnection);
```

### Immutable State Updates

Always use immutable patterns when updating state or objects. Never mutate objects directly.

❌ Incorrect

```typescript
const state = { name: "John", age: 30 };
state.name = "Jane"; // Direct mutation
```

✅ Correct

```typescript
const state = { name: "John", age: 30 };
const newState = { ...state, name: "Jane" };
```

For arrays:

```typescript
// ❌ Incorrect
items.push(newItem);

// ✅ Correct
const newItems = [...items, newItem];
```

### Early Returns

Use early returns to flatten nested if statements. This makes code more readable and reduces cognitive load.

❌ Incorrect

```typescript
function processConnection(connection: Connection) {
  if (connection.status === "active") {
    if (connection.hasError) {
      return "error";
    } else {
      return "success";
    }
  } else {
    return "inactive";
  }
}
```

✅ Correct

```typescript
function processConnection(connection: Connection) {
  if (connection.status !== "active") {
    return "inactive";
  }

  if (connection.hasError) {
    return "error";
  }

  return "success";
}
```

## Styling

### When to Use Inline Styles

Inline styles should only be used as a last resort when there is no other way to style a component. Follow this order when styling:

1. **Style props first** - Use component style props if available (e.g., `<FlexContainer gap="md" />`)
2. **className second** - Use CSS modules with className
3. **Inline style last** - Only when absolutely necessary (e.g., dynamic values from API)

❌ Incorrect

```typescript
// Using inline styles for static values
<div style={{ padding: "16px", display: "flex" }}>
```

✅ Correct

```typescript
// Using layout components with props
<FlexContainer gap="md">
```

### Layout Components

We have two layout components for common styling patterns:

#### FlexContainer

Use `FlexContainer` for flexbox layouts. It provides props for all common flexbox properties.

```typescript
import { FlexContainer } from "components/ui/Flex";

<FlexContainer
  direction="row"              // "row" | "column" | "row-reverse" | "column-reverse"
  gap="md"                     // "none" | "xs" | "sm" | "md" | "lg" | "xl" | "2xl"
  alignItems="center"          // "flex-start" | "flex-end" | "center" | "baseline" | "stretch"
  justifyContent="space-between" // "flex-start" | "flex-end" | "center" | "space-between" | "space-around" | "space-evenly"
  wrap="nowrap"                // "wrap" | "nowrap" | "wrap-reverse"
>
  {children}
</FlexContainer>
```

#### Box

Use `Box` for adding spacing (padding and margin) to components without creating custom CSS.

```typescript
import { Box } from "components/ui/Box";

<Box
  p="lg"    // padding (all sides)
  py="md"   // padding y-axis (top and bottom)
  px="sm"   // padding x-axis (left and right)
  pt="xs"   // padding-top
  pr="md"   // padding-right
  pb="lg"   // padding-bottom
  pl="xl"   // padding-left
  m="md"    // margin (all sides)
  my="sm"   // margin y-axis (top and bottom)
  mx="lg"   // margin x-axis (left and right)
  mt="xs"   // margin-top
  mr="md"   // margin-right
  mb="lg"   // margin-bottom
  ml="xl"   // margin-left
>
  {children}
</Box>
```

Available spacing sizes: `"none"` | `"xs"` | `"sm"` | `"md"` | `"lg"` | `"xl"` | `"2xl"`

### Using Existing Variables

Before setting any custom values, check if there's an existing variable in our SCSS files:

- **`src/scss/_variables.scss`** - Spacing, sizes, borders, font sizes, etc.
- **`src/scss/_colors.scss`** - Color palette and theme colors
- **`src/scss/_z-indices.scss`** - Z-index values for layering

```scss
// ✅ Correct - using existing variables
@use "scss/variables";
@use "scss/colors";

.myComponent {
  padding: variables.$spacing-md;
  background-color: colors.$blue-400;
  border-radius: variables.$border-radius;
}
```

### Checking for Existing Classes

Before creating a new CSS class, search the codebase for existing classes that might already serve the same purpose. Reusing existing styles promotes consistency and reduces bundle size.

Use your IDE's search or grep to find similar class names:
- Search for keywords related to the styling you need
- Check similar components for reusable patterns
- Look in `src/scss/` for global utility classes

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
