# Kuiper health, backup, and recovery

Run these commands from the repository root on the Linux Compose host. Use Docker Compose v2, Bash, curl, jq, GNU tar, and sha256sum. Use the same Kuiper configuration and compatible MongoDB and Elasticsearch versions on the restore host.

## Service health and runtime limits

Use `docker compose up -d --wait` to start the stack. Compose waits for MongoDB, Redis, and Elasticsearch before starting Flask and the worker. Nginx waits for Flask. Use `docker compose ps` to see which service is unhealthy and `docker compose logs --tail=100 SERVICE` to diagnose it. Flask's `/healthz` checks the HTTP process. The backing services and the Celery worker have separate probes.

| Setting | Default |
| --- | --- |
| `KUIPER_CELERY_CONCURRENCY` | 2 parser jobs |
| `KUIPER_CELERY_CPUS` | 2 CPUs |
| `KUIPER_CELERY_MEMORY_LIMIT` | 4 GiB |
| `KUIPER_FLASK_MEMORY_LIMIT` | 2 GiB |
| `KUIPER_ES_MEMORY_LIMIT` | 6 GiB |
| `KUIPER_ES_JAVA_OPTS` | `-Xms4g -Xmx4g` |

Set overrides in the Compose environment and leave Elasticsearch memory above the Java heap so the node has room for native memory and filesystem cache. Compose restarts exited containers. An unhealthy container that remains running requires operator attention.

## Take a backup

1. Pause uploads and other writers. Wait for pending parsing jobs to finish, then stop Flask, Celery, and Nginx. Leave MongoDB and Elasticsearch running.
2. Ensure `backups/snapshots` exists and is writable by Elasticsearch (UID 1000 in the supplied image). If Compose uses `KUIPER_SNAPSHOT_DIR`, export the same directory for the script. Ensure the operator can read the evidence and snapshot files. Treat the backup as sensitive case data.
3. Use a new lowercase backup ID and run the commands below.

```sh
docker compose stop nginx flask celery
bash scripts/kuiper-backup.sh backup case-backup-01
docker compose up -d --wait
```

Keep `backups/case-backup-01/` and `backups/snapshots/case-backup-01/` together, or copy the corresponding snapshot directory from `KUIPER_SNAPSHOT_DIR`. The first contains the MongoDB dump, raw and extracted evidence, and checksums. The second contains the Elasticsearch snapshot repository. Elasticsearch snapshots require open indices, so the script temporarily opens closed case indices and restores their closed state afterward. If backup fails, check the logged error and confirm those indices are closed before resuming service.

Store the deployment configuration and any custom parser files separately. Redis task state is disposable for this workflow. Drain the queue before backup and use empty Redis storage when restoring.

## Restore to a clean stack

1. Start with empty MongoDB, Elasticsearch and Redis data directories and empty `kuiper/files/files` and `kuiper/files/raw` directories. Copy both backup directories into place. Keep Flask, Celery, Nginx and other writers stopped.
2. Start the backing services and run the restore.

```sh
docker compose up -d --wait mongodb es01 redis
bash scripts/kuiper-backup.sh restore case-backup-01
docker compose up -d --wait
```

The script checks both sets of checksums before writing data. If restore fails after writing starts, keep writers stopped and retry on a fresh target.

Open representative cases in the UI. Compare artifact statuses and Elasticsearch counts with the source, check an inactive case, and compare SHA-256 hashes of sampled raw and extracted evidence. Confirm the indexed records still belong to the expected machines. The ingestion workflow runs these checks on a synthetic case after restoring to fresh storage.

See [Compose startup ordering](https://docs.docker.com/compose/how-tos/startup-order/) and [Elasticsearch snapshot and restore](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore).
