# AWS Helpers

A collection of AWS helper functions.

## Available Tasks


### Replacing EC2 ECS host

> Currently assumes there is a single host in an ASG

This will:

1. Remove scale-in protection from current instance.
2. Scale-out ASG, set min/max/desired count to 2 (new instance will have scale-in protection).
3. Store running tasks from current instance.
4. Ask for confirmation to proceed (check if instance created).
5. Wait until new ecs host  is 'active'.
6. Mark old ecs host as 'draining'.
7. Wait until old host as 0 active tasks, and new host has number of tasks from (3), above.
8. Ask for confirmation to to scale-in.
9. Scale-in ASG, set min/max/desired back to 1. 

Usage `python manage.py replace_ecs_host my-cluster-name --profile my-profile --region eu-west-1`
