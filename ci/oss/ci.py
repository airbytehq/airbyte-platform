
from typing import Any, Awaitable, List, Optional

from aircmd.actions.asyncutils import gather
from aircmd.models.base import PipelineContext
from aircmd.models.click_commands import ClickCommandMetadata, ClickFlag, ClickGroup
from aircmd.models.click_params import ParameterType
from aircmd.models.plugins import DeveloperPlugin
from aircmd.models.utils import make_pass_decorator
from dagger import Client, Container
from prefect import Flow, flow
from prefect.client.schemas import FlowRun, State
from prefect.utilities.annotations import quote

from .settings import OssSettings
from .tasks import (
    build_oss_backend_task,
    build_oss_frontend_task,
    build_storybook_oss_frontend_task,
    test_oss_backend_task,
    test_oss_frontend_task,
)

pass_pipeline_context = make_pass_decorator(PipelineContext, ensure=True)
pass_global_settings = make_pass_decorator(OssSettings, ensure=True)

oss_group = ClickGroup(group_name="oss", group_help="Commands for developing Airbyte OSS")
backend_group = ClickGroup(group_name="backend", group_help="Commands for developing Airbyte OSS backend")
frontend_group = ClickGroup(group_name="frontend", group_help="Commands for developing Airbyte OSS frontend")

class CICommand(ClickCommandMetadata):
    command_name: str = "ci"
    command_help: str = "frontend && backend build and test no helm, acceptance or e2e"
    flags: List[ClickFlag] = [ClickFlag(name = "--scan", 
                                        type = ParameterType.BOOL, 
                                        help = "Enables gradle scanning",
                                        default = False)]

class BuildCommand(ClickCommandMetadata):
    command_name: str = "build"
    command_help: str = "gradlew assemble && pnpm build and test and storybook:build"
    flags: List[ClickFlag] = [ClickFlag(name = "--scan", 
                                        type = ParameterType.BOOL,
                                        help = "Enables gradle scanning", 
                                        default = False)]

class BackendBuildCommand(ClickCommandMetadata):
    command_name: str = "build"
    command_help: str = "gradlew assemble"
    flags: List[ClickFlag] = [ClickFlag(name = "--scan", 
                                        type = ParameterType.BOOL,
                                        help = "Enables gradle scanning", 
                                        default = False)]
    

class BackendTestCommand(ClickCommandMetadata):
    command_name: str = "test"
    command_help: str = "gradlew assemble and gradlew test"
    flags: List[ClickFlag] = [ClickFlag(name = "--scan", 
                                        type = ParameterType.BOOL,
                                        help = "Enables gradle scanning", 
                                        default = False)]

class FrontendBuildCommand(ClickCommandMetadata):
    command_name: str = "build"
    command_help: str = "pnpm build and storybook build and test"


class TestCommand(ClickCommandMetadata):
    command_name: str = "test"
    command_help: str = "gradlew assemble, test && pnpm build, test, storybook build"
    flags: List[ClickFlag] = [ClickFlag(name = "--scan", 
                                        type = ParameterType.BOOL,
                                        help = "Enables gradle scanning", 
                                        default = False)]

@oss_group.command(BuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Build")
async def build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Awaitable[Container]]:
    client.pipeline(BuildCommand().command_name) if client else ctx.get_dagger_client().pipeline(BuildCommand().command_name) 
    results = await gather(backend_build, frontend_build, args=[(), ()], kwargs=[{'scan': scan}, {}])
    return results

@backend_group.command(BackendBuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Backend Build")
async def backend_build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Awaitable[Container]]:
    backend_build_client = client.pipeline("OSS Backend Build") if client else ctx.get_dagger_client().pipeline("OSS Backend Build") 
    backend_build_result = await build_oss_backend_task.submit(settings, ctx, backend_build_client, scan)
    return [backend_build_result]

@frontend_group.command(FrontendBuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Frontend Build")
async def frontend_build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None) -> List[Awaitable[Container]]:
    frontend_build_client = client.pipeline("OSS Frontend Build") if client else ctx.get_dagger_client().pipeline("OSS Frontend Build") 
    frontend_storybook_result = await build_storybook_oss_frontend_task.submit(settings, ctx, frontend_build_client)
    frontend_build_result = await build_oss_frontend_task.submit(settings, ctx, frontend_build_client)
    # note that we technically are running tests during the build phase here, which doesn't make immediate sense
    # but they run on the source code rather than the built artifacts and are faster than the build so it makes more sense to run them in parallel
    # with the build than to run them after the build
    frontend_test_result = await test_oss_frontend_task.submit(settings, ctx, frontend_build_client)
    return [frontend_storybook_result, frontend_build_result, frontend_test_result]

@oss_group.command(TestCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Test")
async def test(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Awaitable[Container]]:
    test_client = client.pipeline(TestCommand().command_name) if client else ctx.get_dagger_client().pipeline(TestCommand().command_name) 
    build_results = await build(scan=scan)
    test_results = await test_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0][0], settings=settings, ctx=quote(ctx), scan=scan)
    # TODO: add cypress E2E tests here
    return [test_results]

@backend_group.command(BackendTestCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Backend Test")
async def backend_test(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Awaitable[Container]]:
    test_client = client.pipeline(TestCommand().command_name) if client else ctx.get_dagger_client().pipeline(TestCommand().command_name) 
    build_results = await build(scan=scan)
    test_results = await test_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0][0], settings=settings, ctx=quote(ctx), scan=scan)
    return [test_results]


# this is a hook
async def print_results(flow:Flow, flow_run: FlowRun, state: State[Any]) -> None:
    state.result()
    print(state.result())
    print(state.state_details)

@oss_group.command(CICommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS CI", on_completion=[print_results])
async def ci(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> None:
    ci_results = await test(scan=scan)
    return [ci_results]


oss_ci_plugin = DeveloperPlugin(name = "oss_ci", base_dirs=["oss", "airbyte-platform-internal"])
oss_ci_plugin.add_group(oss_group)
oss_group.add_group(backend_group)
oss_group.add_group(frontend_group)




