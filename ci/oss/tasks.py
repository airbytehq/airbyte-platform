


import os

from aircmd.actions.environments import (
    with_global_dockerd_service,
    with_gradle,
    with_node,
    with_pnpm,
)
from aircmd.actions.pipelines import get_repo_dir, sync_to_gradle_cache_from_homedir
from aircmd.models.base import PipelineContext
from aircmd.models.settings import load_settings
from dagger import CacheSharingMode, CacheVolume, Client, Container
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
    
    gradle_command = ["./gradlew", "assemble", "-x", "buildDockerImage", "-x", "dockerBuildImage", "publishtoMavenLocal", "--build-cache", "--no-daemon"]

    # Now we execute the build inside the container. Each step is analagous to a layer in buildkit
    # Be careful what is passed in here, as anything like a timestamp will invalidate the whole cache.
    result = (with_gradle(client, ctx, settings, sources_to_include=files_from_host, directory=base_dir)
                .with_service_binding("dockerd", dockerd_service)
                .with_(load_settings(client, settings))
                .with_env_variable("VERSION", "dev")
                .with_workdir("/airbyte/oss" if base_dir == "oss" else "/airbyte")
                .with_exec(["./gradlew", ":airbyte-config:specs:downloadConnectorRegistry", "--rerun", "--build-cache", "--no-daemon"])
                .with_exec(gradle_command + ["--scan"] if scan else gradle_command)
                .with_(sync_to_gradle_cache_from_homedir(settings.GRADLE_CACHE_VOLUME_PATH, settings.GRADLE_HOMEDIR_PATH)) #TODO: Move this to a context manager
        )
    await result.sync()
    if scan:
        scan_file_contents: str = await result.file("/airbyte/oss/scan-journal.log").contents()
        await create_link_artifact(
            key="gradle-build-scan-oss-build",
            link=scan_file_contents.split(' - ')[2].strip(),
            description="Gradle build scan for OSS Backend Build",
        )


    return result

@task
async def build_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    airbyte_repo_cache: CacheVolume = client.cache_volume("airbyte-repo-cache")

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(client, settings))
                .with_mounted_cache("./build/airbyte-repository", airbyte_repo_cache, sharing=CacheSharingMode.LOCKED)
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "license-check"])
                .with_exec(["pnpm", "run", "validate-lock"])
                .with_exec(["pnpm", "build"]))
    await result.sync()
    return result

@task
async def test_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(client, settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "test:ci"]))

    await result.sync()
    return result


@task
async def test_oss_e2e_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(client, settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "test:ci"]))
    result.sync()
    return result

@task
async def build_storybook_oss_frontend_task(settings: OssSettings, ctx: PipelineContext, client: Client) -> Container:

    result = (with_node(client, settings.airbyte_webapp_node_version)
                .with_(with_pnpm(client, settings.airbyte_webapp_pnpm_version))
                .with_mounted_directory("/airbyte", get_repo_dir(client, settings, ".", include=frontend_files))
                .with_workdir("/airbyte/oss/airbyte-webapp" if base_dir == "oss" else "/airbyte/airbyte-webapp")
                .with_(load_settings(client, settings))
                .with_exec(["pnpm", "install"])
                .with_exec(["pnpm", "run", "build:storybook"]))
    await result.sync()
    return result

@task
async def check_oss_backend_task(client: Client, oss_build_result: Container, settings: OssSettings, ctx: PipelineContext, scan: bool) -> Container:
    gradle_command = ["./gradlew", "check", "-x", "test", "-x", ":airbyte-webapp:check","-x", "buildDockerImage", "-x", "dockerBuildImage", "--build-cache", "--no-daemon"]
    files_from_result = [
                        "**/build.gradle", 
                        "**/gradle.properties", 
                        "**/settings.gradle",
                        "**/airbyte-*/**/*"
                        ]
    result = ( 
            with_gradle(client, ctx, settings, directory=base_dir)
            .with_directory("/root/.m2/repository", oss_build_result.directory("/root/.m2/repository")) # published jar files from mavenLocal
            .with_directory("/airbyte/oss", oss_build_result.directory("/airbyte/oss"), include=files_from_result)
            .with_workdir("/airbyte/oss" if base_dir == "oss" else "/airbyte")
            .with_(load_settings(client, settings))
            .with_env_variable("VERSION", "dev") 
            .with_env_variable("METRIC_CLIENT", "") # override 'datadog' value for metrics-lib test
            .with_exec(gradle_command + ["--scan"] if scan else gradle_command) 
            .with_(sync_to_gradle_cache_from_homedir(settings.GRADLE_CACHE_VOLUME_PATH, settings.GRADLE_HOMEDIR_PATH))
        )
    await result.sync()
    

@task
async def test_oss_backend_task(client: Client, oss_build_result: Container, settings: OssSettings, ctx: PipelineContext, scan: bool) -> Container:
    files_from_result = [
                            "**/build.gradle", 
                            "**/gradle.properties", 
                            "**/settings.gradle",
                            "**/airbyte-*/**/*"
                            ]
    
    #TODO: There isn't a way to manage services lifecycle here yet. We need
    # https://github.com/dagger/dagger/pull/5557. Once we get that, we can refactor this
    # to reuse the same service container with start and stop commands. Until then we need
    # to spin up 3 separate containers on different ports, which adds a fair amount of boilerplate
    airbyte_proxy_service = ( # service binding for airbyte-proxy-test-container
        client.container()  
        .from_("nginx:latest")
        .with_directory("/", oss_build_result.directory("/airbyte/oss"), include="**/airbyte-proxy/**")
        .with_workdir("airbyte-proxy")
        .with_env_variable("BASIC_AUTH_PROXY_TIMEOUT", settings.basic_auth_proxy_timeout)
        .with_exec(["apt-get", "update"])
        .with_exec(["apt-get", "install", "-y", "apache2-utils"]) # needed for htpasswd
        .with_exec(["mkdir", "-p", "/etc/nginx/templates"])
        .with_exec(["cp", "401.html", "/etc/nginx/401.html"])
        .with_exec(["chmod", "+x", "run.sh"])
    )

    airbyte_proxy_service_auth = (
        airbyte_proxy_service
        .with_env_variable("PROXY_TEST", "AUTH")
        .with_env_variable("BASIC_AUTH_USERNAME", settings.basic_auth_username)
        .with_env_variable("BASIC_AUTH_PASSWORD", settings.basic_auth_password)
        .with_exposed_port(8000)
        .with_exec(["cp", "nginx-test-auth.conf.template", "/etc/nginx/templates/nginx-test-auth.conf.template"])
        .with_exec(["./run.sh"])
    )

    airbyte_proxy_service_newauth = (
        airbyte_proxy_service
        .with_env_variable("PROXY_TEST", "NEW_AUTH")
        .with_env_variable("BASIC_AUTH_USERNAME", settings.basic_auth_username)
        .with_env_variable("BASIC_AUTH_PASSWORD", settings.basic_auth_updated_password)
        .with_exposed_port(8001)
        .with_exec(["cp", "nginx-test-auth-newpass.conf.template", "/etc/nginx/templates/nginx-test-auth-newpass.conf.template"])
        .with_exec(["./run.sh"])  
    )

    airbyte_proxy_service_noauth = (
        airbyte_proxy_service
        .with_env_variable("PROXY_TEST", "NO_AUTH")
        .with_exposed_port(8002)
        .with_exec(["cp", "nginx-test-no-auth.conf.template", "/etc/nginx/templates/nginx-test-no-auth.conf.template"])
        .with_exec(["./run.sh"])
    )
    
    gradle_command = ["./gradlew", "test", "-x", ":airbyte-webapp:test","-x", "buildDockerImage", "-x", "dockerBuildImage", "--build-cache", "--no-daemon"]

    result = ( 
                with_gradle(client, ctx, settings, directory=base_dir)
                .with_service_binding("airbyte-proxy-test-container", airbyte_proxy_service_auth)
                .with_service_binding("airbyte-proxy-test-container-newauth", airbyte_proxy_service_newauth)
                .with_service_binding("airbyte-proxy-test-container-noauth", airbyte_proxy_service_noauth)
                .with_directory("/root/.m2/repository", oss_build_result.directory("/root/.m2/repository")) # published jar files from mavenLocal
                .with_directory("/airbyte/oss", oss_build_result.directory("/airbyte/oss"), include=files_from_result)
                .with_workdir("/airbyte/oss" if base_dir == "oss" else "/airbyte")
                .with_(load_settings(client, settings))
                .with_env_variable("VERSION", "dev") 
                .with_env_variable("METRIC_CLIENT", "") # override 'datadog' value for metrics-lib test
                .with_exec(gradle_command + ["--scan"] if scan else gradle_command) 
                .with_(sync_to_gradle_cache_from_homedir(settings.GRADLE_CACHE_VOLUME_PATH, settings.GRADLE_HOMEDIR_PATH))
            )
    await result.sync()
    if scan:
        scan_file_contents: str = await result.file("/airbyte/oss/scan-journal.log").contents()
        await create_link_artifact(
            key="gradle-build-scan-oss-test",
            link=scan_file_contents.split(' - ')[2].strip(),
            description="Gradle build scan for OSS Backend Tests",
        )

    return result
