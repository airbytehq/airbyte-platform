package io.airbyte.config.secrets.persistence;

import com.azure.identity.IntelliJCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import io.airbyte.config.secrets.SecretCoordinate;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
@Property(name = "airbyte.secret.persistence", value = "azure_secret_manager")
@Property(name = "airbyte.secret.store.azure.key-vault-url", value = "https://poc-a7h-kv.vault.azure.net/")
public class AzureSecretManagerPersistenceTest {

    @Inject
    SecretPersistence persistence;

    @Test
    public void testWrite() {
        var coordinate = new SecretCoordinate("plop", 1);
        var secret = persistence.read(coordinate);
        assertTrue(secret.isEmpty());
        persistence.write(coordinate, "my secret !");
        secret = persistence.read(coordinate);
        assertEquals(secret, "my secret !");
    }

}
