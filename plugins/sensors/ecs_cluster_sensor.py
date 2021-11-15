from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from functools import reduce

class ECSClusterSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,
                 cluster_name=None,
                 auto_scaling_group=None,
                 desired_capacity=None,
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.auto_scaling_group = auto_scaling_group
        self.aws_conn_id = aws_conn_id
        self.desired_capacity = desired_capacity
        self.__hook = None
        self.__ecs_client = None
        self.__autoscaling_client = None

    @property
    def hook(self) -> AwsHook:
        if not self.__hook:
            self.__hook = AwsHook(
                aws_conn_id=self.aws_conn_id,
                client_type='ecs'
            )
        return self.__hook

    @property
    def ecs_client(self):
        if not self.__ecs_client:
            self.__ecs_client = self.hook.get_client_type('ecs')
        return self.__ecs_client
    
    def list_container_instances(self):
        return self.ecs_client.list_container_instances(
            cluster=self.cluster_name,
            status='ACTIVE'
        )      

    @property
    def autoscaling_client(self):
        if not self.__autoscaling_client:
            self.__autoscaling_client = self.hook.get_client_type('autoscaling')
        return self.__autoscaling_client
      
    def describe_auto_scaling_group(self):
        return self.autoscaling_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.auto_scaling_group],
            MaxRecords=100
        )

    def poke(self, context):
        self.log.info(f'poking ecs cluster %s for status', self.cluster_name)
        self.log.info(f'poking auto scale group %s for status', self.auto_scaling_group)

        ecs_container_instances = self.list_container_instances()
        self.log.info(ecs_container_instances)

        container_instances = ecs_container_instances.get('containerInstanceArns', [])
        cluster_instances_count = len(container_instances)

        auto_scaling_group = self.describe_auto_scaling_group()
        instances = auto_scaling_group.get('AutoScalingGroups')[0].get('Instances')
        auto_scaling_group_instances_count = len(instances)

        capacity = reduce(lambda cp, cn: cp+cn, list(map(lambda i:int(i.get('WeightedCapacity')), instances))) if auto_scaling_group_instances_count > 0 else 0
        test_a = (cluster_instances_count >= auto_scaling_group_instances_count)
        test_b = (capacity >= self.desired_capacity)
        
        self.log.info('cluster_instances_count %s', cluster_instances_count)
        self.log.info('auto_scaling_group_instances_count %s', auto_scaling_group_instances_count)
        self.log.info('capacity %s out of desired capacity %s', capacity, self.desired_capacity)
        return test_a and test_b