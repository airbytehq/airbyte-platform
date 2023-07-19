
from typing import Awaitable, Optional

from aircmd.models.base import PipelineContext
from aircmd.models.click_commands import ClickCommandMetadata, ClickGroup
from aircmd.models.plugins import DeveloperPlugin
from aircmd.models.utils import make_pass_decorator
from dagger import Client, Container
from prefect import flow
from prefect.utilities.annotations import quote

from .settings import OssSettings
from .tasks import build_oss_task, test_oss_backend_task

pass_pipeline_context = make_pass_decorator(PipelineContext, ensure=True)
pass_global_settings = make_pass_decorator(OssSettings, ensure=True)

oss_group = ClickGroup(group_name="oss", group_help="Commands for developing Airbyte OSS")

class CICommand(ClickCommandMetadata):
    command_name: str = "ci"
    command_help: str = "Runs CI for Airbyte OSS"


class BuildCommand(ClickCommandMetadata):
    command_name: str = "build"
    command_help: str = "Builds Airbyte OSS"


class TestCommand(ClickCommandMetadata):
    command_name: str = "test"
    command_help: str = "Tests Airbyte OSS"


@oss_group.command(BuildCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Build")
async def build(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None) -> Awaitable[Container]:
    build_client = client.pipeline(BuildCommand().command_name) if client else ctx.get_dagger_client().pipeline(BuildCommand().command_name) 
    build_result = await build_oss_task.submit(settings, ctx, build_client)
    return build_result.result()


@oss_group.command(TestCommand())
@pass_global_settings
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS Test")
async def test(settings: OssSettings, ctx: PipelineContext, client: Optional[Client] = None) -> None:
    test_client = client.pipeline(TestCommand().command_name) if client else ctx.get_dagger_client().pipeline(TestCommand().command_name) 
    build_result = await build()
    test_result = await test_oss_backend_task.submit(client=test_client, cloud_build_result=await build_result, settings=settings, ctx=quote(ctx))
    return test_result.result()


@oss_group.command(CICommand())
@pass_pipeline_context
@flow(validate_parameters=False, name="OSS CI")
async def ci(ctx: PipelineContext) -> None:
    pass



oss_ci_plugin = DeveloperPlugin(name = "oss_ci", base_dirs=["oss", "airbyte-platform-internal"])
oss_ci_plugin.add_group(oss_group)

