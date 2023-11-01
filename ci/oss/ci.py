
from typing import List, Optional

from aircmd.actions.asyncutils import gather
from aircmd.models.base import PipelineContext
from aircmd.models.click_commands import (
    ClickCommandMetadata,
    ClickFlag,
    ClickGroup,
)
from aircmd.models.click_params import ParameterType
from aircmd.models.click_utils import LazyPassDecorator
from aircmd.models.plugins import DeveloperPlugin
from dagger import Client, Container
from prefect import State, flow
from prefect.utilities.annotations import quote

from .settings import OssSettings
from .tasks import (
    build_oss_backend_task,
    build_oss_frontend_task,
    build_storybook_oss_frontend_task,
    check_oss_backend_task,
    test_oss_backend_task,
    test_oss_frontend_task,
)

settings = OssSettings()
pass_pipeline_context = LazyPassDecorator(PipelineContext, global_settings = settings)
pass_global_settings = LazyPassDecorator(OssSettings)

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
async def oss_build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Container]:
    client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name)
    results: List[List[State]] = await gather(backend_build, frontend_build, args=[(), ()], kwargs=[{'scan': scan, 'client': client},{'client': client}])
    return results

@backend_group.command(BackendBuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Backend Build")
async def backend_build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Container]:
    backend_build_client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name)
    backend_build_result = await build_oss_backend_task.submit(settings, ctx, backend_build_client, scan)
    return [backend_build_result] # we wrap this in a list to make it composable with gather upstream so we don't mix Container with List[Container]

@frontend_group.command(FrontendBuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Frontend Build")
async def frontend_build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None) -> List[Container]:
    frontend_build_client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name)
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
async def oss_test(settings: OssSettings, ctx: PipelineContext, build_results: Optional[List[Container]] = None, client: Optional[Client] = None, scan: bool = False) -> List[Container]:
    test_client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name) 
    build_results = await oss_build(scan=scan, client=test_client) if build_results is None else build_results
    test_results = await test_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0][0], settings=settings, ctx=quote(ctx), scan=scan)
    await check_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0][0], settings=settings, ctx=quote(ctx), scan=scan)
    # TODO: add cypress E2E tests here
    return [test_results] 

@backend_group.command(BackendTestCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Backend Test")
async def backend_test(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> Container:
    test_client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name) 
    build_results = await backend_build(scan=scan, client=test_client)
    test_results = await test_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0], settings=settings, ctx=quote(ctx), scan=scan)
    await check_oss_backend_task.submit(client=test_client, oss_build_result=build_results[0], settings=settings, ctx=quote(ctx), scan=scan)

    return test_results

@oss_group.command(CICommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS CI")
async def oss_ci(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None, scan: bool = False) -> List[Container]:
    ci_client = await ctx.get_dagger_client(client, ctx.prefect_flow_run_context.flow.name) 
    ci_results = await oss_test(scan=scan, client=ci_client)
    return ci_results


oss_ci_plugin = DeveloperPlugin(name = "oss_ci", base_dirs=["oss", "airbyte-platform-internal"])
oss_ci_plugin.add_group(oss_group)
oss_group.add_group(backend_group)
oss_group.add_group(frontend_group)




