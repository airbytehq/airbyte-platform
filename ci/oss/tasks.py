


import os

from aircmd.actions.environments import (
    with_global_dockerd_service,
    with_gradle,
    with_node,
    with_pnpm,
)
from aircmd.actions.pipelines import get_repo_dir
from aircmd.models.base import PipelineContext
from aircmd.models.utils import load_settings
from dagger import CacheVolume, Client, Container
from prefect import task
from prefect.artifacts import create_link_artifact

from .settings import OssSettings

# Determine the base directory
base_dir = None if os.getcwd().endswith('oss') else "oss"

frontend_files = ["**/airbyte-api/**/config.yaml",
                     "**/airbyte-api/**/cloud-config.yaml",
                     "**/airbyte-connector-builder-resources/**/CDK_VERSION",
                     "**/**openapi.yaml",
                     "**/airbyte-webapp/**/*"]


@task
async def build_oss_backend_task(settings: OssSettings, ctx: PipelineContext, client: Client, scan) -> Container:
    dockerd_service = with_global_dockerd_service(client, settings)

    ctx.dockerd_service = dockerd_service

    # This is the list of files needed to run. Keep this as minimal as possible to avoid accidentally invalidating the cache
    files_from_host=["airbyte-*/**/*"]
    
    gradle_command = ["./gradlew", "assemble", "-x", "buildDockerImage", "-x", "dockerBuildImage", "publishtoMavenLocal", "--build-cache"]

    # Now we execute the build inside the container. Each step is analagous to a layer in buildkit
    # Be careful what is passed in here, as anything like a timestamp will invalidate the whole cache.
    result = (with_gradle(client, ctx, settings, sources_to_include=files_from_host, directory=base_dir)
                .with_service_binding("dockerd", dockerd_service)
                .with_(load_settings(settings))
                .with_env_variable("VERSION", "dev")
                .with_workdir("/airbyte/oss" if base_dir == "oss" else "/airbyte")
                .with_exec(["./gradlew", ":airbyte-config:specs:downloadConnectorRegistry", "--rerun", "--build-cache"])
                .with_exec(["pwd"])
                .with_exec(gradle_command + ["--scan"] if scan else gradle_command)
                .with_exec(["rsync", "-az", "/root/.gradle/", "/root/gradle-cache"])
        )
    
    if scan:
        artifact = await create_link_artifact(
            key="gradle-build-scan",
            link=result.file("/airbyte/oss/scan-journal.log").contents().split(' - ')[2].strip(),
            description="Gradle build scan",
        )
        print(artifact)

    return result.sync()

@task
async def build_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    airbyte_repo_cache: CacheVolume = client.cache_volume("airbyte-repo-cache")

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(settings))
                .with_mounted_cache("./build/airbyte-repository", airbyte_repo_cache)
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "build"]))
    return result.sync()

@task
async def test_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "test:ci"]))


    return result.sync()


@task
async def test_oss_e2e_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "test:ci"]))

    return result.sync()

@task
async def build_storybook_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "build:storybook"]))

    return result.sync()


@task
async def test_oss_backend_task(client: Client, oss_build_result: Container, settings: OssSettings, ctx: PipelineContext, scan: bool) -> Container:

    files_from_result = [
                            "**/build.gradle", 
                            "**/gradle.properties", 
                            "**/settings.gradle",
                            "**/airbyte-*/**/*"
                            ]
    

    ( # service binding for airbyte-proxy-test-container
        client.container()
        .from_("nginx:latest")
        #.with_directory("/", oss_build_result.directory("airbyte-proxy"))
        .with_env_variable("BASIC_AUTH_USERNAME", settings.basic_auth_username)
        .with_env_variable("BASIC_AUTH_PASSWORD", settings.basic_auth_password)
        .with_env_variable("BASIC_AUTH_PROXY_TIMEOUT", settings.basic_auth_proxy_timeout)
        .with_env_variable("PROXY_PASS_WEB", "http://localhost")
        .with_env_variable("PROXY_PASS_API", "http://localhost")
        .with_env_variable("CONNECTOR_BUILDER_SERVER_API", "http://localhost")
        .with_exposed_port(80)
    )
    
    gradle_command = ["./gradlew", "test", "-x", ":airbyte-webapp:test", "-x", "buildDockerImage", "-x", "airbyte-proxy:bashTest", "-x", ":airbyte-metrics:metrics-lib:test", "--build-cache"]

    result = (
                with_gradle(client, ctx, settings, directory=base_dir)
                #.with_service_binding("airbyte-proxy-test-container1", airbyte_proxy_service_1)
                .with_directory("/root/.m2/repository", oss_build_result.directory("/root/.m2/repository")) # published jar files from mavenLocal
                .with_directory("/airbyte/oss", oss_build_result.directory("/airbyte/oss"), include=files_from_result)
                .with_workdir("/airbyte/oss" if base_dir == "oss" else "/airbyte")
                .with_(load_settings(settings))
                .with_env_variable("VERSION", "dev")
                #TODO: Wire airbyte-proxy tests in with services
                #TODO: Investigate the one failing test at :airbyte-metrics:metrics-lib:test
                .with_exec(gradle_command + ["--scan"] if scan else gradle_command) 

        )
    
        
    if scan:
        await create_link_artifact(
            key="gradle-build-scan",
            link=result.file("/airbyte/oss/scan-journal.log").contents().split(' - ')[2].strip(),
            description="Gradle build scan",
        )
    return result.sync()
