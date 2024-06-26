package io.airbyte.config.secrets.persistence;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.IntelliJCredential;
import com.azure.identity.IntelliJCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import io.airbyte.config.secrets.SecretCoordinate;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.time.Instant;
import java.time.ZoneOffset;

/*
https://learn.microsoft.com/en-us/java/api/overview/azure/security-keyvault-secrets-readme?view=azure-java-stable
https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable#defaultazurecredential
 */
@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^azure_secret_manager$")
@Named("secretPersistence")
public class AzureSecretManagerPersistence implements SecretPersistence {

    @Value("${airbyte.secret.store.azure.key-vault-url}")
    private String keyVaultUrl;

    // FIXME bazint create a client each time ?
    private SecretClient secretClient;

    @Override
    public void initialize() {
        secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                // new ManagedIdentityCredentialBuilder().build()
                .credential(
                //        new DefaultAzureCredentialBuilder().build()
                        new IntelliJCredentialBuilder().build()
                )
                .buildClient();
    }

    @Override
    public String read(SecretCoordinate coordinate) {
        try {
            return secretClient.getSecret(coordinate.getCoordinateBase()).getValue();
        } catch (ResourceNotFoundException e) {
            return "";
        }
    }

    @Override
    public void write(SecretCoordinate coordinate, String payload) {
        writeWithExpiry(coordinate, payload, null);
    }

    @Override
    public void writeWithExpiry(SecretCoordinate coordinate, String payload, Instant expiry) {
        var secret = new KeyVaultSecret(coordinate.getCoordinateBase(), payload);
        if (expiry != null)
            secret.getProperties().setExpiresOn(expiry.atOffset(ZoneOffset.UTC));
        secretClient.setSecret(secret);
    }

    @Override
    public void delete(SecretCoordinate coordinate) {
        // KeyVaultErrorException: Status code 409,
        // "{"error":{"code":"Conflict","message":"Secret plop is currently in a deleted but recoverable state, and its name cannot be reused;
        // in this state, the secret can only be recovered or purged.","innererror":{"code":"ObjectIsDeletedButRecoverable"}}}"
        var poller = secretClient.beginDeleteSecret(coordinate.getCoordinateBase());
        poller.poll();
        poller.waitForCompletion();
    }

    @Override
    public void disable(SecretCoordinate coordinate) {
    }

    public static void main(String[] args) {
        AzureSecretManagerPersistence persistence = new AzureSecretManagerPersistence();
        persistence.keyVaultUrl = "https://poc-a7h-kv.vault.azure.net/";
        persistence.initialize();
        persistence.write(new SecretCoordinate("plop", 1), "my secret !");
        // persistence.delete(new SecretCoordinate("plop", 1));
    }

}
