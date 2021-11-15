from aws_cdk import core, custom_resources as custom
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_mwaa as mwaa
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
from  helper_functions import helpers
from aws_cdk.aws_ec2 import Vpc, SecurityGroup
 

class MwaaCdkStackEnv(core.Stack):

    def __init__(self, scope: core.Construct, id: str, mwaa_props,  **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
       
        vpc = Vpc.from_lookup(self, id="VPC", vpc_id='vpc-39ecbd5f')
        # Create MWAA S3 Bucket and upload local dags 
        
        dags_bucket = s3.Bucket(
            self,
            "mwaa-dags",
            bucket_name=f"{mwaa_props['dagss3location'].lower()}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        
        

        dags_bucket_arn = dags_bucket.bucket_arn
        helpers.execut(AWS_S3_PLUGINS_DIR='plugins.zip')
        # Create MWAA IAM Policies and Roles, copied from MWAA documentation site

        mwaa_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:airflow:{self.region}:{self.account}:environment/{mwaa_props['mwaa_env']}"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:ListAllMyBuckets"
                    ],
                    effect=iam.Effect.DENY,
                    resources=[
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}"
                        ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}"
                        ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-{mwaa_props['mwaa_env']}-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:DescribeLogGroups"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey*",
                        "kms:Encrypt",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "kms:ViaService": [
                                f"sqs.{self.region}.amazonaws.com",
                                f"s3.{self.region}.amazonaws.com",
                            ]
                        }
                    },
                ),
            ]
        )

        mwaa_service_role = iam.Role(
            self,
            "mwaa-service-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
            inline_policies={"CDKmwaaPolicyDocument": mwaa_policy_document},
            path="/service-role/"
        )


        # Create MWAA Security Group and get networking info

        self.security_group = SecurityGroup.from_security_group_id(
            scope=self,
            id="sg-0e1f33096dbca1530",
            security_group_id="sg-0e1f33096dbca1530"
        )

        security_group_id = self.security_group.security_group_id
        
        self.security_group.connections.allow_internally(ec2.Port.all_traffic(),"MWAA")
        

        s3deploy.BucketDeployment(self, "DeployDAG",
            sources=[s3deploy.Source.asset("./dags")],
            destination_bucket=dags_bucket,
            destination_key_prefix="dags"
        )
        s3deploy.BucketDeployment(self, "DeployReq",
            sources=[s3deploy.Source.asset("./mwaa_requirements", exclude= ['**', '!requirements.txt'])],
            destination_bucket=dags_bucket,
            destination_key_prefix="Requirements"
        )
        subnets = ['subnet-021be1ecdd1fe43cf','subnet-05c33e6136f5beee9']
        network_configuration = mwaa.CfnEnvironment.NetworkConfigurationProperty(
            security_group_ids=[security_group_id],
            subnet_ids=subnets,
        )

        # **OPTIONAL** Configure specific MWAA settings - you can externalise these if you want

        logging_configuration = mwaa.CfnEnvironment.LoggingConfigurationProperty(
            task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO")
            )

        options = {
            'core.load_default_connections': False,
            'core.load_examples': False,
            'webserver.dag_default_view': 'tree',
            'webserver.dag_orientation': 'TB'
        }

        tags = {
            'env': f"{mwaa_props['mwaa_env']}",
            'service': 'MWAA Apache AirFlow'
        }

        
        # Create MWAA environment using all the info above

        managed_airflow = mwaa.CfnEnvironment(
            scope=self,
            id='airflow-test-environment',
            name=f"{mwaa_props['mwaa_env']}",
            airflow_configuration_options={'core.default_timezone': 'utc','core.dag_concurrency':128,'core.parallelism':512},
            airflow_version='2.0.2',
            dag_s3_path="dags",
            environment_class='mw1.medium',
            execution_role_arn=mwaa_service_role.role_arn,
            logging_configuration=logging_configuration,
            max_workers=25,
            min_workers=5,
            schedulers=2,
            network_configuration=network_configuration,
            #plugins_s3_object_version=plugin_obj_version.get_response_field("VersionId"),
            # plugins_s3_path="plugins.zip",
            #requirements_s3_object_version=req_obj_version.get_response_field("VersionId"),
            # requirements_s3_path="Requirements/requirements.txt",
            source_bucket_arn=dags_bucket_arn,
            webserver_access_mode='PUBLIC_ONLY',
            #weekly_maintenance_window_start=None
        )

        managed_airflow.add_override('Properties.AirflowConfigurationOptions', options)
        managed_airflow.add_override('Properties.Tags', tags)

        core.CfnOutput(
            self,
            id="MWAASecurityGroup",
            value=security_group_id,
            description="Security Group name used by MWAA"
        )
        


    



