import boto3
import time
import fire


class Operations:
    """A collection of operations for managing AWS estate"""

    def __init__(self, profile='default', region='eu-west-1'):
        """
        Create new operations object
        :param profile: AWS profile
        :param region: AWS region
        """
        session = boto3.Session(profile_name=profile, region_name=region)
        self._ecs = session.client('ecs')
        self._autoscale = session.client('autoscaling')

    def replace_ecs_host(self, cluster_name: str):
        """
        Replace ECS host instance by scaling-out ASG, waiting and scaling-in
        """
        # get arns for cluster
        active_instances = self._ecs.list_container_instances(cluster=cluster_name, status='ACTIVE')
        arns = active_instances.get('containerInstanceArns')

        # get instance_ids for cluster/arn
        container_instances = self._ecs.describe_container_instances(cluster=cluster_name, containerInstances=arns)
        instances = container_instances.get('containerInstances')

        if len(instances) > 1:
            print('More than 1 instance found... will need tweaked to handle')
            return

        arn = arns[0]
        target_instance = instances[0]
        instance_id = target_instance.get('ec2InstanceId')

        pending_tasks = target_instance.get('pendingTasksCount')

        if pending_tasks > 0:
            print(f'Instance "{instance_id}" has pending tasks, bailing')
            return

        running_tasks = target_instance.get('runningTasksCount')

        # get asg for instances
        asg_instances = self._autoscale.describe_auto_scaling_instances(InstanceIds=[instance_id])
        asg_instance = asg_instances.get('AutoScalingInstances')[0]

        asg_name = asg_instance.get('AutoScalingGroupName')

        # remove scale in protection
        if not asg_instance.get('ProtectedFromScaleIn'):
            print(f'Instance "{instance_id}" in asg "{asg_name}" does not have scale-in protection, nothing to do..')
        else:
            print(f'Removing instance protection from instance "{instance_id}" in asg "{asg_name}"..')
            self._autoscale.set_instance_protection(InstanceIds=[instance_id],
                                                    AutoScalingGroupName=asg_name,
                                                    ProtectedFromScaleIn=False)

        # update ASG max/min/desired
        # TODO - read min/max/desired and +1
        print(f'Updating asg "{asg_name}" to have min/max/desired counts of 2..')
        self._autoscale.update_auto_scaling_group(AutoScalingGroupName=asg_name,
                                                  MinSize=2,
                                                  MaxSize=2,
                                                  DesiredCapacity=2,
                                                  NewInstancesProtectedFromScaleIn=True)

        self._wait_until_state('ACTIVE', f'ec2InstanceId != {instance_id}', cluster_name)

        if not self._yes_or_no('asg updated - new instance should exist. Proceed?'):
            return

        print(f'Setting instance "{instance_id}" in cluster {cluster_name} to DRAINING..')
        self._ecs.update_container_instances_state(cluster=cluster_name,
                                                   containerInstances=[arn],
                                                   status='DRAINING')

        # wait til drained, with 0 tasks
        self._wait_until_state('DRAINING', f'ec2InstanceId == {instance_id}', cluster_name, 0)

        # verify new one is active
        self._wait_until_state('ACTIVE', f'ec2InstanceId != {instance_id}', cluster_name, running_tasks, False)

        if not self._yes_or_no('new instance and active, all tasks drained from old. Remove old instance?'):
            return

        self._autoscale.update_auto_scaling_group(AutoScalingGroupName=asg_name,
                                                  MinSize=1,
                                                  MaxSize=1,
                                                  DesiredCapacity=1)

    def _wait_until_state(self, target_state, container_filter, cluster_name, running_task_target=-1, wait_on_first=True):

        print(f'waiting until container matching filter "{container_filter}" reaches "{target_state}"')

        instance_updated = False
        first = True
        while not instance_updated:
            # if it's not the first time round, always go in
            # if it is the first time - and wait_on_first is false wait
            if (not first) or wait_on_first:
                print("waiting 60s...")
                time.sleep(60)

            first = False

            try:
                # get instance_status for cluster/arn
                instances = self._ecs.list_container_instances(cluster=cluster_name,
                                                               filter=container_filter)
                arns = instances.get('containerInstanceArns')

                container_instances = self._ecs.describe_container_instances(cluster=cluster_name,
                                                                             containerInstances=arns)
                instance = container_instances.get('containerInstances')[0]

                status = instance.get('status')
                if status != target_state:
                    print(f'New instance still in "{status}" state, wanting "{target_state}"')
                    continue

                # status is active, check counts
                running_tasks = instance.get('runningTasksCount')
                if running_task_target != -1 and running_tasks != running_task_target:
                    print(f'New instance has {running_tasks} of a targeted {running_task_target} running tasks')
                    continue

                print(f'New instance has the targeted {running_task_target} running tasks.')
                instance_updated = True
            except Exception as e:
                print(f'Error in _wait_until_state - {e}')

    def _yes_or_no(self, question):
        while "the answer is invalid":
            reply = str(input(f'{question} (y/n): ')).lower().strip()
            if reply[:1] == 'y':
                return True
            if reply[:1] == 'n':
                return False


if __name__ == '__main__':
    fire.Fire(Operations)
    # operation = Operations("dlcsspinup")
    # operation.replace_ecs_host("dlcsspinup-prod")
