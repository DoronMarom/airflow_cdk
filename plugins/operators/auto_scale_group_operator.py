from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AutoScaleGroupOperator(BaseOperator):
    ui_color = '#f0ede4'
    template_fields = ('auto_scaling_group', 'desired_capacity', 'honor_cooldown')

    @apply_defaults
    def __init__(self,
                 aws_conn_id=None,
                 auto_scaling_group=None,
                 desired_capacity=2,
                 honor_cooldown=False,
                 **kwargs):
        super(AutoScaleGroupOperator, self).__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.auto_scaling_group = auto_scaling_group
        self.honor_cooldown = honor_cooldown
        self.desired_capacity = desired_capacity
        self.__hook = None
        self.__client = None

    @property
    def hook(self) -> AwsHook:
        if not self.__hook:
            self.__hook = AwsHook(
                aws_conn_id=self.aws_conn_id,
                client_type='autoscaling'
            )
        return self.__hook

    @property
    def client(self):
        if not self.__client:
            self.__client = self.hook.get_client_type('autoscaling')
        return self.__client

    def execute(self, context):
        try:
            self.log.info(
                f'Set desired capacity for AutoScalingGroup  {self.auto_scaling_group} with {self.desired_capacity} instances and HonorCooldown = {self.honor_cooldown}'
            )
            self.client.set_desired_capacity(
                AutoScalingGroupName=self.auto_scaling_group,
                DesiredCapacity=self.desired_capacity,
                HonorCooldown=self.honor_cooldown,
            )
        except:
            if (self.on_failure_callback): self.on_failure_callback(context)
            raise         