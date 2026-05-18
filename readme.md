# AWS Helpers

A collection of helper scripts for common AWS infrastructure maintenance tasks, using [Fire](https://github.com/google/python-fire) for the CLI interface.

## Usage

```bash
python manage.py <operation> [args] --profile <aws-profile> [--region <region>]
```

`--profile` is required for all operations. `--region` defaults to the profile's configured region if not specified.

---

## `replace_ecs_host`

Replaces running ECS EC2 host instances in an ASG without downtime. Supports clusters with multiple instances.

**Process:**

1. Reads current ASG capacity (min/max/desired).
2. Removes scale-in protection from old instance(s).
3. Doubles ASG desired capacity so new instances are launched (new instances get scale-in protection automatically).
4. Waits until the expected number of new instances are `ACTIVE`.
5. Prompts for confirmation before proceeding.
6. Marks old instances as `DRAINING`.
7. Waits until old instances have 0 running tasks.
8. Prompts for confirmation before removing old instances.
9. Restores ASG to original min/max/desired capacity, terminating the old instances.

**Usage:**

```bash
# abort if there are pending tasks
python manage.py replace_ecs_host my-cluster-name --profile my-profile

# proceed even if there are pending tasks
python manage.py replace_ecs_host my-cluster-name --allow_pending --profile my-profile

# specify a region explicitly
python manage.py replace_ecs_host my-cluster-name --profile my-profile --region eu-west-1
```

---

## `update_rds_instance`

Updates an RDS instance to a newer engine version without downtime by snapshotting and restoring to a new instance, then upgrading in place. The old instance is left running so you can verify before deleting it manually.

**Process:**

1. Takes a snapshot of the source RDS instance.
2. Polls until the snapshot is available.
3. Restores the snapshot to a new instance (all configuration is copied from the source: instance class, AZ, multi-AZ, subnet group, VPC security groups). The new instance name toggles a `-1` suffix (e.g. `foo` → `foo-1`, `foo-1` → `foo`).
4. Polls until the restored instance is available.
5. Builds a multi-hop upgrade path to reach the target version, following `ValidUpgradeTarget` at each step.
6. Chains upgrades sequentially, polling for availability between each hop.
7. Reports completion and reminds you to verify and delete the old instance manually.

**Version behaviour:**

- Always upgrades to the **latest minor version** of the requested major.
- Errors if the current version is already higher than the requested major.
- Errors if the instance is already at the latest minor version of the requested major.
- Automatically chains intermediate hops if required (e.g. 11 → 12 → 13 → 14).

**Confirmation prompts:** Three `y/n` prompts are shown before: taking the snapshot, restoring to the new instance, and starting the upgrade chain.

**Usage:**

```bash
# upgrade to the latest minor version of PostgreSQL 14
python manage.py update_rds_instance my-rds-instance --version 14 --profile my-profile

# specify a custom snapshot name (defaults to {instance-name}-{timestamp})
python manage.py update_rds_instance my-rds-instance --version 14 --snapshot_name my-snapshot --profile my-profile
```
