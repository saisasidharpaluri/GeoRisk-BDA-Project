# AWS scaffold

`georisk-foundation.template.json` defines the reviewed low-cost foundation:
one encrypted lifecycle-managed S3 bucket, two one-shard Kinesis streams, a
Glue database, a short-retention Glue log group, and an Athena workgroup. It
accepts an existing least-privilege Glue role ARN rather than creating a
broad role.

Use least-privilege IAM roles, a dedicated project bucket, short-lived streaming runs, explicit teardown steps, and region/account configuration outside source code. Check current pricing and student-account permissions before deployment.

Deploy only after checking the AWS account, region, service quotas, and
student-credit limits. Delete the stack after the demonstration and verify
that retained S3 objects are no longer needed.

For final evaluation, keep the Phase 7 report and artifact manifest as
evidence, then remove generated data and the CloudFormation stack after the
approved demonstration window.

Phase 8 provides `georisk-deployment-check` as a read-only preflight. It
validates this template's bounded resource settings and the supplied bucket
and Glue role parameters, writes a reviewable deployment plan, and performs
no AWS API calls. Run it successfully before using a separately reviewed
CloudFormation deployment command.
