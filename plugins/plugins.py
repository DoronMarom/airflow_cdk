from airflow.plugins_manager import AirflowPlugin
from hooks.base_aws import AwsBaseHook
from operators.auto_scale_group_operator import AutoScaleGroupOperator
from operators.ecs_operator import ECSOperatorExtended
from operators.ecs import ECSOperator

class auto_scale_group_operator(AutoScaleGroupOperator):
  pass

class ecs_operator(ECSOperator):
      pass

class ecs_extended_operator(ECSOperatorExtended):
      pass

class aws_webhook_hook(AwsBaseHook):
  pass

class aws_plugin(AirflowPlugin):

    name = 'aws_plugin'       
    hooks = [aws_webhook_hook]
    operators = [auto_scale_group_operator, ecs_operator, ecs_extended_operator]
