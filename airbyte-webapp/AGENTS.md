# AGENTS.md — `oss/airbyte-webapp/`

This file contains agent-specific guidance for the React/TypeScript webapp.
Read the repository-level [AGENTS.md](../../AGENTS.md) first, then the
human-and-agent contribution guide in [CONTRIBUTING.md](./CONTRIBUTING.md).
The project overview and setup basics are in [README.md](./README.md).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the webapp's development commands,
architecture conventions, testing guidance, and development-only mocking rules.

## Agent workflow

- Use the repository's pinned Node.js and pnpm versions; run commands from
  `oss/airbyte-webapp/`.
- Do not edit or commit generated API artifacts under
  `src/core/api/generated/`; regenerate them with `pnpm generate-client`.
- Keep changes focused on the requested scope and preserve unrelated local
  modifications.
- After editing, re-read only the changed regions and run the narrowest
  checks that cover the change before the required final verification.

## Where to look

This file records the webapp decisions that nearby code does not show. It
does not restate the docs below. When a fact changes, update the doc that
owns it. Paths are relative to `oss/airbyte-webapp/`.

| Topic                                                                                  | Owner                                                                                                                                |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| API hooks, Orval, TanStack Query, and the `core/api` import boundary                   | `src/core/api/README.md`                                                                                                             |
| Styling, spacing, and tokens                                                           | `STYLEGUIDE.md` ("Spacing and Layout", "Styling")                                                                                    |
| Forms                                                                                  | The JSDoc in `src/components/ui/forms/Form.tsx` and `FormControl.tsx`                                                                |
| Connector-builder `SchemaForm`                                                         | `src/components/ui/forms/SchemaForm/` and its stories                                                                                |
| Toolchain pins, conventions, and the verification command                              | `CONTRIBUTING.md` ("Webapp conventions", "Verification")                                                                             |
| Where a file belongs, OSS/Cloud sharing, and import rules                              | `CONTRIBUTING.md` ("Quick Reference", "Decision Trees: Where to Put Code", "OSS/Cloud Sharing Model", "Import Rules & Restrictions") |
| Exports, component props, TypeScript patterns, and `Next`-prefix experiment components | `STYLEGUIDE.md` ("Exporting", "Component Props", "TypeScript Best Practices", "Launch Darkly Experiments")                           |
| Unit tests and test ids                                                                | `CONTRIBUTING.md` ("Unit tests with Jest") and `STYLEGUIDE.md` ("Testing")                                                           |
| E2E tests                                                                              | `playwright/README.md`                                                                                                               |
| Scope discipline and the base readability rules                                        | The root [AGENTS.md](../../AGENTS.md)                                                                                                |

## Decisions

- **Reuse a primitive from `src/components/ui/` before you write new
  markup.** Many primitives have no story. When a `*.stories.tsx` file
  exists, it shows the intended use. Otherwise, read the existing
  callsites.
- **UI text, including UI error text, uses keys in
  `src/locales/en.json`.** `src/locales/en.errors.json` holds only the
  messages for backend problem types, which `HttpProblem` looks up by
  type. No lint rule catches a hardcoded string in JSX, so you are the
  check.
- **Shared code does not import from `src/cloud/`** (`CONTRIBUTING.md`
  "OSS/Cloud Sharing Model"). No lint rule enforces this, and existing
  code breaks the rule. Do not copy those imports.

## Readability

The root `AGENTS.md` "Code readability" section is the base rule. The
rules below add to it for webapp code, and are stricter on purpose.

- **No nested ternaries**, in JSX or in plain code, because the reader
  must hold every branch in mind at once. A ternary inside a JSX `&&`
  chain counts. Use a small named component, a lookup object, an early
  return, or a named variable.

  ```tsx
  // Before
  <Card>{title && !collapsed && (isNested ? <Text bold>{title}</Text> : <Heading size="sm">{title}</Heading>)}</Card>

  // After: a small component in the same file
  <Card>{title && !collapsed && <GroupTitle title={title} isNested={isNested} />}</Card>
  ```

  ```ts
  // Before
  const size = isLarge ? "lg" : isMedium ? "md" : "sm";

  // After
  const SIZE_BY_VARIANT: Record<Variant, Size> = { large: "lg", medium: "md", small: "sm" };
  const size = SIZE_BY_VARIANT[variant];
  ```

- **A boolean expression with more than three operands does not go
  inline.** Give its parts names, such as `hasRequiredFields`, or move
  them into small predicate helpers. A named part tells the reader which
  rule it checks.

  ```ts
  // Before
  return (
    typeof id === "string" &&
    typeof title === "string" &&
    (description === undefined || typeof description === "string") &&
    (order === undefined || typeof order === "number") &&
    Array.isArray(groups) &&
    groups.every((groupId) => typeof groupId === "string")
  );

  // After
  const isStringList = (value: unknown): value is string[] =>
    Array.isArray(value) && value.every((item) => typeof item === "string");
  const isAbsentOrOfType = (value: unknown, type: "string" | "number") => value === undefined || typeof value === type;

  const hasRequiredFields = typeof id === "string" && typeof title === "string" && isStringList(groups);
  const hasValidOptionalFields = isAbsentOrOfType(description, "string") && isAbsentOrOfType(order, "number");
  return hasRequiredFields && hasValidOptionalFields;
  ```

- **Keep functions atomic.** When a function or handler validates,
  computes, and updates state, extract the pure computation into a
  named function. A test can then call that function alone.

  ```ts
  // Before
  const onSubmit = (values: FormValues) => {
    if (!values.name) {
      return;
    }
    setStepOrder(values.steps.filter((step) => step.enabled).map((step) => step.id));
  };

  // After
  const getEnabledStepIds = (steps: Step[]) => steps.filter((step) => step.enabled).map((step) => step.id);

  const onSubmit = (values: FormValues) => {
    if (!values.name) {
      return;
    }

    setStepOrder(getEnabledStepIds(values.steps));
  };
  ```

- **Match the surrounding style, not its complexity.** Follow nearby
  naming, file layout, and primitives. A nested ternary or a long
  condition in nearby code is not a pattern to copy. Do not rewrite
  that old code either, because the root `AGENTS.md` asks for the
  smallest diff.

## Verify

Before you declare done, run `pnpm verify; echo "exit $?"` and report the
exit code. The last steps print nothing on success, so the final output
line alone does not prove a pass. `CONTRIBUTING.md` "Verification" lists
the steps.

## Self-review before hand-back

Do these steps before you report done:

1. Read your own diff, as the root `AGENTS.md` "Verification discipline"
   describes, and add `git diff` for uncommitted work. Check each line
   you wrote against Readability above, and fix what breaks a rule.
2. If step 1 changed code, run `pnpm verify` again.
3. State in your report that you ran this gate and what it fixed.

## Testing the webapp locally

To run this webapp against production Cloud, a stock OSS backend (`abctl`),
or a local backend, load the
[`airbyte-cloud-e2e-testing`](https://github.com/airbytehq/ai-skills/blob/main/plugins/common-skills/skills/airbyte-cloud-e2e-testing/SKILL.md)
skill from `airbytehq/ai-skills`. Production login only works behind the
`cloud.airbyte.com` origin proxy described there; `https://localhost:3000`
is rejected by production Keycloak.
