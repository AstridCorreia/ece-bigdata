# Lab 2 - Object storage with S3

## Answers

### Is the deletion denied?
No, the file was deleted anyway. Our S3 server (SeaweedFS) only applies bucket policies to anonymous users, not to logged-in users like us.

### Why are the credentials stored in a Secret and not in the ConfigMap?
A ConfigMap is for normal settings and everyone can read it. A Secret is made for private data like passwords and keys, it is better protected. So the S3 keys go in a Secret.

### What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?
The Onyxia keys only work for a short time. Tomorrow they will be expired, so the Job will fail with an "ExpiredToken" error.
In a real company, we don't copy keys by hand. The Job gets its own identity (a service account) and the keys are given and renewed automatically.

### How would you turn this Job into a daily ingestion?
We can use a CronJob instead of a Job. A CronJob runs the Job automatically at a fixed time, for example every day at 2am with the schedule `0 2 * * *`.

### Note
The namespace quota requires a CPU limit on every container, so `limits.cpu: 500m` was added to the Job.