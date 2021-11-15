#!/usr/bin/env python3
import io
import os
from aws_cdk import core
from aws_cdk.core import Environment
from mwaa_cdk.mwaa_cdk_env import MwaaCdkStackEnv


version='v14'
env_EU=core.Environment(
    account=os.environ['CDK_DEFAULT_ACCOUNT'],
    region=os.environ['CDK_DEFAULT_REGION']
)

mwaa_props = {'dagss3location': f'bigabid-airflow-cdk-{version}','mwaa_env' : f'mwaa-cdk-demo-{version}'}

app = core.App()


mwaa_env = MwaaCdkStackEnv(
    scope=app,
    id=f"MWAA-Environment-test-{version}",
    env=env_EU,
    mwaa_props=mwaa_props
)

app.synth()
