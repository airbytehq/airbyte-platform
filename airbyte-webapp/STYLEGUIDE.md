# Frontend Style Guide

This serves as a living document regarding conventions we have agreed upon as a frontend team. In general, the aim of these decisions and discussions is to both (a) increase the readability and consistency of our code and (b) decrease day to day decision-making so we can spend more time writing better code.

## General Code Style and Formatting

 Where possible, we rely on automated systems (ESLint, prettier, stylelint) to maintain consistency in code style. The configuration files including VSCode workspace settings and recommended extensions are checked into our repository, so no individual setup should be required - just open the `airbyte-webapp` folder in your VSCode.

- Don’t use single-character names. Using meaningful name for function parameters is a way of making the code self-documented and we always should do it. Example:
  - `.filter(([key, value]) => isDefined(value.default))` ✅
  - `.filter(([k, v]) => isDefined(v.default))` ❌

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
