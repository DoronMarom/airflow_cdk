from operators.ecs import ECSOperator
from airflow.utils.decorators import apply_defaults
from json import loads, dumps
from airflow.configuration import conf
class ECSOperatorExtended(ECSOperator):
    template_fields = ('working_plan', )

    @apply_defaults
    def __init__(self, working_plan=None, *args, **kwargs):
        self.default_owner = conf.get('operators', 'DEFAULT_OWNER')        
        self.working_plan = working_plan     
        super(ECSOperatorExtended, self).__init__(*args, **kwargs)
  
    def pre_execute(self, context):
        container_overrides = self.overrides.get('containerOverrides')[0]
        self.log.info(container_overrides)
        self.working_plan = None if  self.working_plan == 'None' else self.working_plan
        self.working_plan = loads(self.working_plan)
        raw_prefix = self.working_plan.get("raw_prefix")
        reduce_prefix = self.working_plan.get("reduce_prefix")
        self.log.info("raw_prefix: %s, reduce_prefix: %s", raw_prefix, reduce_prefix)
        environment: list = container_overrides.get('environment')
        environment.extend([{
            "name": "S3_SOURCE_FOLDER",
            "value": raw_prefix
        }, {
            "name": "S3_REDUCE_FOLDER",
            "value": reduce_prefix
        }])
        self.owner = f"{self.default_owner}_{self.dag_id}_{self.task_id}"