# Airbyte Helm Chart

This README provides guidance on how to make changes and contributions to the Airbyte Helm chart.

## Overview

The Airbyte Helm chart uses a configuration generator system to manage environment variables and configuration across different components. This system helps maintain consistency and reduces duplication in the Helm templates.

### Key Components

- **`config.yaml`**: The primary input file that defines environment variables, their corresponding Helm values, and default values.
- **Generated Templates**: Files in `templates/config/_*.tpl` that are automatically generated from `config.yaml`.
- **`via` Tool**: An in-house tool that processes `config.yaml` and generates the template files.

## Configuration System

### How It Works

1. The `config.yaml` file defines configuration "concerns" (ConfigSets) as top-level stanzas.
2. Each ConfigSet generates a corresponding template file in `templates/config/`.
3. These templates are included in the Helm chart's `env-configmap.yaml` and `secret.yaml` files.
4. When the chart is deployed, these configurations are made available to the Airbyte components.

## Making Changes to the Helm Chart

### Adding or Modifying Environment Variables

To add or modify an environment variable:

1. Identify the appropriate section in `config.yaml` for your variable.
2. Add or modify the variable definition with the required fields.
3. Run the generator to update the templates.
4. Test your changes by deploying the chart locally.
5. Add a new empty entry to the `charts/v2/airbyte/values.yaml` file.

Example of adding a variable to an existing section:

```yaml
common:
  keyBasePath: global
  config:
    # Existing variables...
    
    # New variable
    - env: NEW_ENVIRONMENT_VARIABLE
      key: newFeature.enabled
      defaultValue: false
```

### Adding a New Configuration Section

To add a new configuration section:

1. Add a new top-level stanza to `config.yaml`.
2. Define the configuration variables for this section.
3. Run the generator to create the new template file.
4. Include the new template in `env-configmap.yaml` and/or `secret.yaml` as needed.

Example of adding a new section:

```yaml
newFeature:
  keyBasePath: global.newFeature
  config:
    - env: NEW_FEATURE_ENABLED
      key: enabled
      defaultValue: false
      
    - env: NEW_FEATURE_URL
      key: url
      valueExp: (printf "http://%s:%d" .Values.global.newFeature.host (int .Values.global.newFeature.port))
```

After adding this section, you need to include it in the appropriate files:

1. In `env-configmap.yaml`, add:
   ```yaml
   {{- include "airbyte.newFeature.configVars" . | nindent 2 }}
   ```

2. If your section contains sensitive variables (marked with `sensitive: true`), also add to `secret.yaml`:
   ```yaml
   {{- include "airbyte.newFeature.secrets" . | nindent 2 }}
   ```
   
3. Update `charts/v2/airbyte/values.yaml` to include an empty entry for the new section:
   ```yaml
   newFeature: {}
   ```   

### Running the Generator

> NOTE: You will need to [install via](https://github.com/airbytehq/via?tab=readme-ov-file#installation) in order to generate the config templates.

To generate the template files after making changes to `config.yaml`:

```bash
# From the chart directory (oss/charts/v2/airbyte/)
make gen.config
```

This will process `config.yaml` and update or create the template files in `templates/config/`.

## Structure of `config.yaml`

### ConfigSets

Each top-level stanza in `config.yaml` represents a ConfigSet, which is a group of related configuration variables. For example:

```yaml
database:
  keyBasePath: global.database
  config:
    # Database-related variables...
```

### Common Fields

- **`env`**: The environment variable name.
- **`key`**: The corresponding key in the Helm values.
    - By default the root of the path is the name of the config set (e.g. `database`).
    - This can be overriden by the `keyBasePath` field.
- **`defaultValue`**: A static default value.
- **`defaultValueExp`**: An expression to compute the default value.
- **`valueExp`**: An expression to compute the value.
- **`sensitive`**: Flag for sensitive values that should be stored in a secret.
- **`secretRef`**: Reference to a secret for sensitive values.

### Discriminator Fields

Discriminator fields allow for conditional configuration based on certain values. For example:

```yaml
storage:
  keyBasePath: global.storage
  discriminatorField: type
  discriminatorFieldOpts: 
    - azure
    - gcs
    - minio
    - s3
  config: 
    # Common variables...
    
    - env: AWS_ACCESS_KEY_ID
      key: s3.accessKeyId
      discriminatorOpts:
        - s3
      sensitive: true
```

In this example, the `AWS_ACCESS_KEY_ID` variable will only be included when the storage type is set to `s3`.

### Expression Syntax

Expressions in `valueExp` and `defaultValueExp` use Helm template syntax. Common patterns include:

- **String formatting**: `(printf "http://%s:%d" .Values.host (int .Values.port))`
- **Conditional logic**: `ternary "value-if-true" "value-if-false" (eq .Values.condition "value")`
- **Including other templates**: `(include "airbyte.common.airbyteUrl" .)`

## Best Practices

### Testing Changes

Always test your changes by deploying the chart locally before submitting them. This helps catch issues that might not be detected during generation.

### Backward Compatibility

When modifying existing configurations:

- Be cautious about removing or renaming variables, as it may break existing deployments.
- If you need to remove a section, ensure you also remove any includes in the templates.

### Common Pitfalls to Avoid

1. **Forgetting to include templates**: After adding a new section, make sure to include it in the appropriate files (`env-configmap.yaml` and/or `secret.yaml`).
2. **Invalid expressions**: Ensure that expressions in `valueExp` and `defaultValueExp` are valid Helm template syntax and can be embedded within `{{- ... }}`.
3. **Missing required fields**: Each variable definition should have at least `env` and `key` fields.
4. **Not running the generator**: Always run `make gen.config` after making changes to `config.yaml`.

## Examples

### Example 1: Adding a Simple Environment Variable

```yaml
# In config.yaml
logging:
  keyBasePath: global.logging
  config: 
    # Existing variables...
    
    - env: LOG_FORMAT
      key: format
      defaultValue: json
```

### Example 2: Using Expressions for Dynamic Values

```yaml
# In config.yaml
api:
  keyBasePath: global.api
  config:
    - env: API_URL
      key: url
      valueExp: (printf "http://%s-api-svc.%s:%d" .Release.Name .Release.Namespace (int .Values.api.service.port))
```

### Example 3: Using Discriminator Fields

```yaml
# In config.yaml
database:
  keyBasePath: global.database
  discriminatorField: type
  discriminatorFieldOpts:
    - postgres
    - mysql
  config:
    - env: DATABASE_TYPE
      key: type
      defaultValue: postgres
      
    - env: POSTGRES_HOST
      key: postgres.host
      discriminatorOpts:
        - postgres
        
    - env: MYSQL_HOST
      key: mysql.host
      discriminatorOpts:
        - mysql
```

## Contributing

When contributing changes to the Helm chart:

1. Follow the guidelines in this README.
2. Test your changes thoroughly.
3. Document any new configuration options or changes to existing ones.
4. Submit your changes according to the project's contribution guidelines.
