import boto3
import time
import fire
from datetime import datetime


def _version_key(v):
    return [int(x) for x in v.split(".")]


class Operations:
    """A collection of operations for managing AWS estate"""

    def __init__(self, profile="default", region="eu-west-1"):
        """
        Create new operations object
        :param profile: AWS profile
        :param region: AWS region
        """
        session = boto3.Session(profile_name=profile, region_name=region)
        self._ecs = session.client("ecs")
        self._autoscale = session.client("autoscaling")
        self._rds = session.client("rds")

    def replace_ecs_host(self, cluster_name: str, allow_pending=False):
        """
        Replace ECS host instances by scaling-out ASG, waiting and scaling-in
        """
        # get arns for cluster
        arns = self._ecs.list_container_instances(
            cluster=cluster_name, status="ACTIVE"
        ).get("containerInstanceArns")

        instances = self._ecs.describe_container_instances(
            cluster=cluster_name, containerInstances=arns
        ).get("containerInstances")

        old_arns = []
        old_instance_ids = []

        for instance in instances:
            arn = instance.get("containerInstanceArn")
            instance_id = instance.get("ec2InstanceId")
            pending_tasks = instance.get("pendingTasksCount")

            if pending_tasks > 0:
                if allow_pending:
                    print(f'Instance "{instance_id}" has pending tasks')
                else:
                    print(f'Instance "{instance_id}" has pending tasks, aborting')
                    return

            old_arns.append(arn)
            old_instance_ids.append(instance_id)

        instance_count = len(instances)
        print(f"Found {instance_count} instance(s) to replace")

        # get asg name from first instance (all should be in same ASG)
        asg_instances = self._autoscale.describe_auto_scaling_instances(
            InstanceIds=old_instance_ids
        ).get("AutoScalingInstances")
        asg_name = asg_instances[0].get("AutoScalingGroupName")

        # read current ASG config
        asg_details = self._autoscale.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        ).get("AutoScalingGroups")[0]
        original_min = asg_details.get("MinSize")
        original_max = asg_details.get("MaxSize")
        original_desired = asg_details.get("DesiredCapacity")
        original_new_protected = asg_details.get("NewInstancesProtectedFromScaleIn")

        # remove scale-in protection from old instances
        asg_instances_map = {i.get("InstanceId"): i for i in asg_instances}
        protected_ids = []
        for instance_id in old_instance_ids:
            asg_inst = asg_instances_map.get(instance_id, {})
            if not asg_inst.get("ProtectedFromScaleIn"):
                print(
                    f'Instance "{instance_id}" does not have scale-in protection, nothing to do..'
                )
            else:
                print(
                    f'Removing instance protection from "{instance_id}" in asg "{asg_name}"..'
                )
                protected_ids.append(instance_id)
        if protected_ids:
            self._autoscale.set_instance_protection(
                InstanceIds=protected_ids,
                AutoScalingGroupName=asg_name,
                ProtectedFromScaleIn=False,
            )

        # double the ASG capacity
        new_desired = original_desired * 2
        new_max = max(original_max, new_desired)
        print(
            f'Updating asg "{asg_name}" desired from {original_desired} to {new_desired}..'
        )
        self._autoscale.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=original_min,
            MaxSize=new_max,
            DesiredCapacity=new_desired,
            NewInstancesProtectedFromScaleIn=True,
        )

        # wait for new instances to become active
        self._wait_for_new_instances_active(cluster_name, old_arns, instance_count)

        if not self._yes_or_no("ASG updated - new instances should exist. Proceed?"):
            return

        # drain old instances
        print(
            f'Setting {len(old_arns)} old instance(s) to DRAINING in cluster "{cluster_name}"..'
        )
        self._ecs.update_container_instances_state(
            cluster=cluster_name, containerInstances=old_arns, status="DRAINING"
        )

        # wait for old instances to drain
        self._wait_for_instances_drained(cluster_name, old_arns)

        # capture the new instance ids so we can un-protect them once the old
        # instances are removed (their protection was set via the ASG flag)
        new_arns = self._get_new_instance_arns(cluster_name, old_arns)
        new_instances = self._ecs.describe_container_instances(
            cluster=cluster_name, containerInstances=new_arns
        ).get("containerInstances")
        new_instance_ids = [i.get("ec2InstanceId") for i in new_instances]

        if not self._yes_or_no(
            "New instances active, all tasks drained from old. Remove old instances?"
        ):
            return

        # restore original ASG capacity to remove old instances
        print(
            f'Restoring asg "{asg_name}" to original capacity '
            f"(min={original_min}, max={original_max}, desired={original_desired}).."
        )
        self._autoscale.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=original_min,
            MaxSize=original_max,
            DesiredCapacity=original_desired,
            NewInstancesProtectedFromScaleIn=original_new_protected,
        )

        # remove scale-in protection from the surviving new instances
        if new_instance_ids:
            print(
                f"Removing scale-in protection from {len(new_instance_ids)} "
                f"new instance(s).."
            )
            self._autoscale.set_instance_protection(
                InstanceIds=new_instance_ids,
                AutoScalingGroupName=asg_name,
                ProtectedFromScaleIn=False,
            )

    def update_rds_instance(
        self, instance_name: str, version: int, snapshot_name: str = None
    ):
        """
        Update an RDS instance to a new engine version without downtime via snapshot/restore/upgrade
        """
        # generate snapshot name if not provided
        if snapshot_name is None:
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            snapshot_name = f"{instance_name}-{timestamp}"

        # get source instance details
        print(f'Fetching details for RDS instance "{instance_name}"..')
        source = self._rds.describe_db_instances(
            DBInstanceIdentifier=instance_name
        ).get("DBInstances")[0]

        engine = source.get("Engine")
        current_version = source.get("EngineVersion")
        current_major = int(current_version.split(".")[0])
        print(f"Current engine: {engine} {current_version}")

        # validate version direction
        if current_major > version:
            print(
                f"Error: current version {current_version} is higher than requested major version {version}"
            )
            return

        # determine target version (latest minor of requested major)
        target_version = self._get_latest_minor_version(engine, str(version))
        print(f"Target version: {target_version}")

        if current_version == target_version:
            print(
                f"Error: instance is already at the latest minor version {target_version}"
            )
            return

        # toggle -1 suffix for new instance name
        if instance_name.endswith("-1"):
            new_instance_name = instance_name[:-2]
        else:
            new_instance_name = f"{instance_name}-1"
        print(f'New instance name will be: "{new_instance_name}"')

        # build upgrade path up front so it can be shown before any changes are made
        print("Building upgrade path..")
        upgrade_path = self._build_upgrade_path(engine, current_version, target_version)

        # step 1: confirm and take snapshot
        if not self._yes_or_no(
            f'Take snapshot "{snapshot_name}" of "{instance_name}"?'
        ):
            return
        print(f'Creating snapshot "{snapshot_name}" of "{instance_name}"..')
        self._rds.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_name,
            DBInstanceIdentifier=instance_name,
        )

        # step 2: poll until snapshot available
        self._wait_for_snapshot(snapshot_name)

        # step 3: confirm and restore snapshot to new instance
        if not self._yes_or_no(
            f'Restore snapshot to new instance "{new_instance_name}"?'
        ):
            return
        print(
            f'Restoring snapshot "{snapshot_name}" to new instance "{new_instance_name}"..'
        )
        self._restore_from_snapshot(snapshot_name, new_instance_name, source)

        # step 4: poll until restore complete
        self._wait_for_rds_available(new_instance_name)

        # steps 5-6: confirm upgrade plan then chain upgrades
        upgrade_plan = " -> ".join([current_version] + upgrade_path)
        if not self._yes_or_no(
            f'Upgrade "{new_instance_name}" via: {upgrade_plan}?'
        ):
            return
        self._chain_upgrades_from_path(new_instance_name, upgrade_path)

        print(
            f'Instance "{new_instance_name}" has been upgraded to {target_version}.\n'
            f'Old instance "{instance_name}" is still running - verify and delete manually when ready.'
        )

    # --- helpers ---

    def _get_new_instance_arns(self, cluster_name, old_arns):
        """Return ARNs of ACTIVE instances not in old_arns"""
        all_arns = self._ecs.list_container_instances(
            cluster=cluster_name, status="ACTIVE"
        ).get("containerInstanceArns", [])
        return [a for a in all_arns if a not in old_arns]

    def _wait_for_new_instances_active(self, cluster_name, old_arns, expected_count):
        """Poll until expected_count new ACTIVE instances exist"""
        print(f"Waiting for {expected_count} new instance(s) to become ACTIVE..")
        while True:
            print("Waiting 60s...")
            time.sleep(60)
            try:
                new_arns = self._get_new_instance_arns(cluster_name, old_arns)
                if len(new_arns) < expected_count:
                    print(f"Found {len(new_arns)} of {expected_count} new instances")
                    continue
                instances = self._ecs.describe_container_instances(
                    cluster=cluster_name, containerInstances=new_arns
                ).get("containerInstances")
                active_count = sum(
                    1 for i in instances if i.get("status") == "ACTIVE"
                )
                print(f"{active_count} of {expected_count} new instances are ACTIVE")
                if active_count >= expected_count:
                    print(f"All {expected_count} new instance(s) are ACTIVE")
                    return
            except Exception as e:
                print(f"Error waiting for new instances: {e}")

    def _wait_for_instances_drained(self, cluster_name, old_arns):
        """Poll until all specified instances have 0 running tasks"""
        print(f"Waiting for {len(old_arns)} old instance(s) to drain..")
        while True:
            print("Waiting 60s...")
            time.sleep(60)
            try:
                instances = self._ecs.describe_container_instances(
                    cluster=cluster_name, containerInstances=old_arns
                ).get("containerInstances")
                counts = [i.get("runningTasksCount") for i in instances]
                print(f"Running task counts on old instances: {counts}")
                if all(c == 0 for c in counts):
                    print("All old instances drained")
                    return
            except Exception as e:
                print(f"Error waiting for drain: {e}")

    def _get_latest_minor_version(self, engine, major_version):
        """Return the latest minor version string for the given engine and major version"""
        response = self._rds.describe_db_engine_versions(
            Engine=engine,
            EngineVersion=major_version,
        )
        versions = [
            v.get("EngineVersion") for v in response.get("DBEngineVersions", [])
        ]
        if not versions:
            raise ValueError(
                f"No versions found for {engine} major version {major_version}"
            )
        versions.sort(key=_version_key)
        return versions[-1]

    def _build_upgrade_path(self, engine, current_version, target_version):
        """
        Build list of intermediate versions to upgrade through, using ValidUpgradeTarget
        at each hop to find the highest reachable version <= target_version.
        """
        path = []
        version = current_version
        target_parts = [int(x) for x in target_version.split(".")]

        while version != target_version:
            response = self._rds.describe_db_engine_versions(
                Engine=engine,
                EngineVersion=version,
            )
            valid_targets = (
                response.get("DBEngineVersions", [{}])[0].get("ValidUpgradeTarget", [])
            )

            candidates = [
                t.get("EngineVersion")
                for t in valid_targets
                if _version_key(t.get("EngineVersion")) <= target_parts
            ]

            if not candidates:
                available = [t.get("EngineVersion") for t in valid_targets]
                raise ValueError(
                    f"No valid upgrade path from {version} to {target_version}. "
                    f"Available targets from {version}: {available}"
                )

            candidates.sort(key=_version_key)
            next_version = candidates[-1]
            path.append(next_version)
            version = next_version

        return path

    def _restore_from_snapshot(self, snapshot_name, new_instance_name, source):
        """Restore a snapshot to a new instance preserving source configuration"""
        kwargs = {
            "DBInstanceIdentifier": new_instance_name,
            "DBSnapshotIdentifier": snapshot_name,
            "DBInstanceClass": source.get("DBInstanceClass"),
            "AvailabilityZone": source.get("AvailabilityZone"),
            "MultiAZ": source.get("MultiAZ"),
            "PubliclyAccessible": source.get("PubliclyAccessible"),
            "AutoMinorVersionUpgrade": source.get("AutoMinorVersionUpgrade"),
            "DBSubnetGroupName": source.get("DBSubnetGroup", {}).get(
                "DBSubnetGroupName"
            ),
            "Engine": source.get("Engine"),
        }
        vpc_sg_ids = [
            sg.get("VpcSecurityGroupId")
            for sg in source.get("VpcSecurityGroups", [])
            if sg.get("Status") == "active"
        ]
        if vpc_sg_ids:
            kwargs["VpcSecurityGroupIds"] = vpc_sg_ids

        self._rds.restore_db_instance_from_db_snapshot(**kwargs)

    def _chain_upgrades_from_path(self, instance_name, upgrade_path):
        """Upgrade instance through a pre-built list of versions"""
        for next_version in upgrade_path:
            print(f'Upgrading "{instance_name}" to {next_version}..')
            self._rds.modify_db_instance(
                DBInstanceIdentifier=instance_name,
                EngineVersion=next_version,
                ApplyImmediately=True,
                AllowMajorVersionUpgrade=True,
            )
            self._wait_for_rds_available(instance_name)
            print(f"Upgrade to {next_version} complete")

    def _wait_for_snapshot(self, snapshot_name):
        """Poll until snapshot status is available"""
        print(f'Waiting for snapshot "{snapshot_name}" to become available..')
        while True:
            print("Waiting 60s...")
            time.sleep(60)
            try:
                response = self._rds.describe_db_snapshots(
                    DBSnapshotIdentifier=snapshot_name
                )
                status = response.get("DBSnapshots", [{}])[0].get("Status")
                print(f"Snapshot status: {status}")
                if status == "available":
                    print("Snapshot is available")
                    return
            except Exception as e:
                print(f"Error checking snapshot: {e}")

    def _wait_for_rds_available(self, instance_name):
        """Poll until RDS instance status is available"""
        print(f'Waiting for RDS instance "{instance_name}" to become available..')
        while True:
            print("Waiting 60s...")
            time.sleep(60)
            try:
                response = self._rds.describe_db_instances(
                    DBInstanceIdentifier=instance_name
                )
                status = response.get("DBInstances", [{}])[0].get("DBInstanceStatus")
                print(f"Instance status: {status}")
                if status == "available":
                    print(f'Instance "{instance_name}" is available')
                    return
            except Exception as e:
                print(f"Error checking instance status: {e}")

    def _yes_or_no(self, question):
        while True:
            reply = str(input(f"{question} (y/n): ")).lower().strip()
            if reply[:1] == "y":
                return True
            if reply[:1] == "n":
                return False


if __name__ == "__main__":
    fire.Fire(Operations)
