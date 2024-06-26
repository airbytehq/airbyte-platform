package io.airbyte.config.secrets.persistence;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.IntelliJCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import io.airbyte.config.secrets.SecretCoordinate;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Inject;
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

    private final String keyVaultUrl;

    @Inject
    private final AzureKeyVaultClientBuilder clientBuilder;

    public AzureSecretManagerPersistence(@Value("${airbyte.secret.store.azure.key-vault-url}") String keyVaultUrl,
                                         AzureKeyVaultClientBuilder clientBuilder) {
        this.keyVaultUrl = keyVaultUrl;
        this.clientBuilder = clientBuilder;
    }

    @Override
    public void initialize() {
    }

    @Override
    public String read(SecretCoordinate coordinate) {
        try {
            return clientBuilder.build(keyVaultUrl).getSecret(coordinate.getCoordinateBase()).getValue();
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
        clientBuilder.build(keyVaultUrl).setSecret(secret);
    }

    @Override
    public void delete(SecretCoordinate coordinate) {
        // KeyVaultErrorException: Status code 409,
        // "{"error":{"code":"Conflict","message":"Secret plop is currently in a deleted but recoverable state, and its name cannot be reused;
        // in this state, the secret can only be recovered or purged.","innererror":{"code":"ObjectIsDeletedButRecoverable"}}}"
        var poller = clientBuilder.build(keyVaultUrl).beginDeleteSecret(coordinate.getCoordinateBase());
        poller.poll();
        poller.waitForCompletion();
    }

    @Override
    public void disable(SecretCoordinate coordinate) {
    }

    public interface AzureKeyVaultClientBuilder {
        SecretClient build(String keyVaultUrl);
    }

    @Singleton
    @Requires(notEnv = Environment.TEST)
    public static class ClientBuilder implements AzureKeyVaultClientBuilder {

        @Override
        public SecretClient build(String keyVaultUrl) {
            return new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new ManagedIdentityCredentialBuilder().build())
                    .buildClient();
        }

    }

    @Singleton
    @Requires(env = Environment.TEST)
    public static class TestClientBuilder implements AzureKeyVaultClientBuilder {

        @Override
        public SecretClient build(String keyVaultUrl) {
            return new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new IntelliJCredentialBuilder().build())
                    .buildClient();
        }

    }

}
