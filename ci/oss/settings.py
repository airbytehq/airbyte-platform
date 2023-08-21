from typing import Optional

from aircmd.models.base import GlobalSettings
from pydantic import Field

from .actions.utils import extract_engine_versions


class OssSettings(GlobalSettings):   
     version: str = Field("dev", env="VERSION")     
     testcontainers_ryuk_disabled: str = Field("true", env="TESTCONTAINERS_RYUK_DISABLED")     
     #docker_host: str = Field("tcp://docker:2375", env="DOCKER_HOST")                                                                                                        
     job_main_container_memory_request: Optional[str] = Field(None, env="JOB_MAIN_CONTAINER_MEMORY_REQUEST")                                                    
     job_main_container_memory_limit: Optional[str] = Field(None, env="JOB_MAIN_CONTAINER_MEMORY_LIMIT")                                                        
     metric_client: Optional[str] = Field(None, env="METRIC_CLIENT")                                                                                                                                                                                                        
     database_user: str = Field("docker", env="DATABASE_USER")                                                                                                  
     database_password: str = Field("docker", env="DATABASE_PASSWORD")                                                                                          
     database_host: str = Field("db", env="DATABASE_HOST")                                                                                                      
     database_port: int = Field(5432, env="DATABASE_PORT")                                                                                                      
     database_db: str = Field("airbyte", env="DATABASE_DB")                                                                                                     
     database_url: str = Field("jdbc:postgresql://db:5432/airbyte", env="DATABASE_URL")                                                                                                                                                  
     config_root: str = Field("/data", env="CONFIG_ROOT")                                                                                                       
     data_docker_mount: str = Field("airbyte_data", env="DATA_DOCKER_MOUNT")                                                                                    
     db_docker_mount: str = Field("airbyte_db", env="DB_DOCKER_MOUNT")                                                                                          
     temporal_host: str = Field("airbyte-temporal:7233", env="TEMPORAL_HOST")                                                                                   
     workspace_root: str = Field("/tmp/workspace", env="WORKSPACE_ROOT")                                                                                        
     workspace_docker_mount: str = Field("airbyte_workspace", env="WORKSPACE_DOCKER_MOUNT")                                                                     
     local_root: str = Field("/tmp/airbyte_local", env="LOCAL_ROOT")                                                                                            
     local_docker_mount: str = Field("/tmp/airbyte_local", env="LOCAL_DOCKER_MOUNT")                                                                            
     hack_local_root_parent: str = Field("/tmp", env="HACK_LOCAL_ROOT_PARENT")                                                                                  
     tracking_strategy: str = Field("segment", env="TRACKING_STRATEGY")                                                                                         
     deployment_mode: str = Field("CLOUD", env="DEPLOYMENT_MODE")                                                                                               
     webapp_url: str = Field("http://localhost:8000/", env="WEBAPP_URL")                                                                                        
     log_level: str = Field("INFO", env="LOG_LEVEL")                                                                                                            
     s3_log_bucket: Optional[str] = Field(None, env="S3_LOG_BUCKET")                                                                                            
     s3_log_bucket_region: Optional[str] = Field(None, env="S3_LOG_BUCKET_REGION")                                                                              
     aws_access_key_id: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")                                                                                    
     aws_secret_access_key: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")                                                                            
     s3_minio_endpoint: Optional[str] = Field(None, env="S3_MINIO_ENDPOINT")                                                                                    
     s3_path_style_access: Optional[str] = Field(None, env="S3_PATH_STYLE_ACCESS")                                                                              
     gcs_log_bucket: Optional[str] = Field(None, env="GCS_LOG_BUCKET")                                                                                          
     job_main_container_cpu_request: Optional[str] = Field(None, env="JOB_MAIN_CONTAINER_CPU_REQUEST")                                                          
     job_main_container_cpu_limit: Optional[str] = Field(None, env="JOB_MAIN_CONTAINER_CPU_LIMIT")                                                          
     config_database_user: Optional[str] = Field(None, env="CONFIG_DATABASE_USER")                                                                              
     config_database_password: Optional[str] = Field(None, env="CONFIG_DATABASE_PASSWORD")                                                                      
     config_database_url: Optional[str] = Field(None, env="CONFIG_DATABASE_URL")                                                                                                                                                                    
     api_server_svc_host: str = Field("airbyte-server", env="API_SERVER_SVC_HOST")                                                                              
     api_server_svc_port: int = Field(8001, env="API_SERVER_SVC_PORT")  
     airbyte_proxy_test_container_port: int = Field(18000, env="AIRBYTE_PROXY_TEST_CONTAINER_PORT")
     basic_auth_username: str = Field("airbyte", env="BASIC_AUTH_USERNAME")
     basic_auth_password: str = Field("password", env="BASIC_AUTH_PASSWORD")
     basic_auth_updated_password: str = Field("pa55w0rd", env="BASIC_AUTH_UPDATED_PASSWORD")
     basic_auth_proxy_timeout: str = Field(120, env="BASIC_AUTH_PROXY_TIMEOUT")
     airbyte_webapp_node_version: str = Field(extract_engine_versions("oss/airbyte-webapp/package.json").get("node"), env="AIRBYTE_WEBAPP_NODE_VERSION")
     airbyte_webapp_pnpm_version: str = Field(extract_engine_versions("oss/airbyte-webapp/package.json").get("pnpm"), env="AIRBYTE_WEBAPP_PNPM_VERSION")

     
