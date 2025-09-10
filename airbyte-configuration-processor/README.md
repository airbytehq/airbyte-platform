# airbyte-configuration-processor

This module contains a custom [KSP Symbol Processor](https://kotlinlang.org/docs/ksp-overview.html) that generates
Micronaut configuration properties with defaults for all `airbyte.` prefixed configuration referenced in the
[airbyte-commons-micronaut](../airbyte-commons-micronaut) module.

The processor scans the module that it is applied to for any classes annotated with the 
`io.micronaut.context.annotation.ConfigurationProperties` annotation.
It uses the annotation to discover the configuration prefix and any fields declared in the annotated class as the
properties that should be generated.  If the field contains a Kotlin default value, that value will be used
in the generated configuration YAML file.  Otherwise, a sensible default will be chosen based on the field's
type (e.g. empty string for a String data type, etc).

In the case where a field points at a constant, that constant must start with the `DEFAULT_` prefix for its name 
to the processor to properly resolve the constant to the value that should be used in the generated
configuration.

The output of the processor is an `airbyte-configuration.yml` written to the `build/generated/ksp/main/resources/`
directory of the processed module.

## Usage

The custom [KSP Symbol Processor](https://kotlinlang.org/docs/ksp-overview.html) will produce a Micronaut configuration
properties YAML file with default values for all `airbyte.` prefixed configuration.  The processor will automatically
run as part of the build for any module that applies this module:

```kotlin
dependencies {
    ksp(project(":oss:airbyte-configuration-processor"))
}

```

However, if you wish to run it manually, use the following Gradle command:

```shell
./gradlew :<project>:kspKotlin  --rerun --info
```