# airbyte-config:config-secrets

This module contains the logic for persisting and hydrating configuration secrets with a secret store.

## Supported Secret Stores

* Local Airbyte Database
* [AWS Secret Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)
* [Google Secret Manager](https://cloud.google.com/secret-manager)
* [HashiCorp Vault](https://www.vaultproject.io/)

## Configuration

To enable one of the secret managers, set the `SECRET_PERSISTENCE` environment variable to one of the following values:

* `AWS_SECRET_MANAGER`
* `GOOGLE_SECRET_MANAGER`
* `TESTING_CONFIG_DB_TABLE`
* `VAULT`
* `NO_OP`

### AWS Secret Manager Configuration

In addition to setting the secret persistence provider, set the following environment variables to configure the
AWS secret manager:

* `AWS_ACCESS_KEY` - Specifies the AWS access key associated with an IAM account.
* `AWS_SECRET_KEY` - Specifies the secret key associated with the access key.  This is essentially the "password" for the access key.

### Google Secret Manager Configuration

In addition to setting the secret persistence provider, set the following environment variables to configure the
Google secret manager:

* `SECRET_STORE_GCP_CREDENTIALS` - Specifies the Google Cloud service account public key associated with the Google Secret Manager.
* `SECRET_STORE_GCP_PROJECT_ID` - Specifies the unique identifier for the Google Cloud project associated with the Google Secret Manager.

### Local Airbyte Database Configuration

In addition to setting the secret persistence provider, set the following environment variables to configure the
local database secret manager:

* `DATABASE_PASSWORD` - Specifies the password associated with the Airbyte Database user account.
* `DATABASE_USER` - Specifies the user account with read/write access to the Airbyte Database.
* `DATABASE_URL` - Specifies the URL of the Airbyte Database.

### Vault Configuration

In addition to setting the secret persistence provider, set the following environment variables to configure the
Vault secret manager:

* `VAULT_ADDRESS` - Specifies the address of the Vault server instance to which API calls should be sent.
* `VAULT_AUTH_TOKEN` - Specifies the token used to access Vault.
* `VAULT_PREFIX` - Specifies the Vault path prefix from which to read.  All secrets will be written/retrieved from under this path.

## Development

In order to use this module in a service at runtime, add the following dependencies to the service:

```kotlin
    implementation(project(':airbyte-config:config-secrets'))

    // Required for local database secret hydration
    runtimeOnly(libs.hikaricp)
    runtimeOnly(libs.h2.database)
````

Next, add the following configuration to the service's `application.yml` file:

```yaml
airbyte:
  secret:
    persistence: ${SECRET_PERSISTENCE}
    store:
      aws:
        access-key: ${AWS_ACCESS_KEY:}
        secret-key: ${AWS_SECRET_ACCESS_KEY:}
      gcp:
        credentials: ${SECRET_STORE_GCP_CREDENTIALS:}
        project-id: ${SECRET_STORE_GCP_PROJECT_ID:}
      vault:
        address: ${VAULT_ADDRESS:}
        prefix: ${VAULT_PREFIX:}
        token: ${VAULT_AUTH_TOKEN:}
```

To support the [local Airbyte database secret manager](#local-database-configuration) variant, also add the `local-secret` database configuration:

```yaml
datasources:
  local-secrets:
    connection-test-query: SELECT 1
    connection-timeout: 30000
    idle-timeout: 600000
    initialization-fail-timeout: -1 # Disable fail fast checking to avoid issues due to other pods not being started in time
    maximum-pool-size: 20
    minimum-idle: 0
    url: ${DATABASE_URL}
    driverClassName: org.postgresql.Driver
    username: ${DATABASE_USER}
    password: ${DATABASE_PASSWORD}
    pool-name: config-pool

jooq:
  datasources:
    local-secrets:
      jackson-converter-enabled: true
      sql-dialect: POSTGRES
```

Typically, the above configuration would be put into a different environment application YAML file (e.g. `application-test.yml`) and
activated by a specific [`MICRONAUT_ENVIRONMENT` profile](https://docs.micronaut.io/latest/guide/#environments).