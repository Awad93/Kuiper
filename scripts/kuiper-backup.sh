#!/usr/bin/env bash
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."

fail() { printf '%s\n' "$*" >&2; exit 1; }

action=${1:-}
id=${2:-}
[[ $action == backup || $action == restore ]] || fail "Usage: bash scripts/kuiper-backup.sh {backup|restore} BACKUP_ID"
[[ $id =~ ^[a-z0-9][a-z0-9_-]{0,63}$ ]] || fail "Use a lowercase backup ID containing letters, digits, underscores or hyphens."
[[ $id != snapshots ]] || fail "The ID snapshots is reserved."
metadata="$PWD/backups/$id"
repository="kuiper_$id"
snapshot_dir="${KUIPER_SNAPSHOT_DIR:-./backups/snapshots}/$id"
es_url=${KUIPER_ES_URL:-http://127.0.0.1:9200}

es() {
    curl -fsS --connect-timeout 5 --max-time "${KUIPER_BACKUP_TIMEOUT:-3600}" \
        -H 'Content-Type: application/json' "$@" "$es_url/${es_path}"
}

mongo() { docker compose exec -T mongodb "$@"; }

running=$(docker compose ps --status running --services flask celery nginx)
[[ -z $running ]] ||
    fail "Stop Flask, Celery and Nginx before taking or restoring a backup."
db_name=$(mongo printenv MONGODB_DB_NAME | tr -d '[:space:]')
[[ $db_name =~ ^[a-zA-Z0-9_-]+$ ]] || fail "Unexpected MongoDB database name."

repository_registered=false
reclose=
unregister() {
    if [[ $repository_registered == true ]]; then
        es_path="_snapshot/$repository" es -X DELETE | jq -e '.acknowledged' >/dev/null
        repository_registered=false
    fi
}

close_indices() {
    if [[ -n $reclose ]]; then
        es_path="$reclose/_close" es -X POST | jq -e '.acknowledged' >/dev/null
        reclose=
    fi
}

cleanup() {
    close_indices || true
    unregister || true
}
trap cleanup EXIT

closed_indices() {
    jq -er 'if all(.[]; test("^[a-z0-9_][a-z0-9_-]*$")) then join(",") else error("Unexpected closed case index name") end' "$metadata/closed-indices.json"
}

register() {
    local readonly=$1
    local settings
    settings=$(jq -nc --arg id "$id" --argjson readonly "$readonly" \
        '{type:"fs",settings:{location:$id,readonly:$readonly}}')
    es_path="_snapshot/$repository" es -X PUT -d "$settings" | jq -e '.acknowledged' >/dev/null
    repository_registered=true
}

if [[ $action == backup ]]; then
    [[ ! -e $snapshot_dir ]] || fail "Snapshot directory already exists."
    mkdir -p backups
    mkdir "$metadata"
    mongo mongodump --db "$db_name" --archive > "$metadata/mongodb.archive"
    printf '%s\n' "$db_name" > "$metadata/mongodb.database"
    tar -C kuiper/files -cf "$metadata/evidence.tar" files raw
    es_path='_cat/indices?format=json&expand_wildcards=all&h=index,status' es |
        jq '[.[] | select(.status == "close" and (.index | startswith(".") | not)) | .index]' > "$metadata/closed-indices.json"
    reclose=$(closed_indices)
    if [[ -n $reclose ]]; then
        es_path="$reclose/_open?wait_for_active_shards=1" es -X POST |
            jq -e '.acknowledged and .shards_acknowledged' >/dev/null
    fi
    register false
    es_path="_snapshot/$repository/kuiper?wait_for_completion=true" \
        es -X PUT -d '{"indices":"*,-.*","ignore_unavailable":false,"include_global_state":false}' |
        jq -e '.snapshot.state == "SUCCESS"' >/dev/null
    unregister
    close_indices
    (
        cd "$snapshot_dir"
        find . -type f -print0 | sort -z | xargs -0 sha256sum
    ) > "$metadata/snapshot.sha256"
    (
        cd "$metadata"
        sha256sum mongodb.archive mongodb.database evidence.tar snapshot.sha256 closed-indices.json > SHA256SUMS
    )
else
    (cd "$metadata" && sha256sum --check SHA256SUMS)
    [[ $(cat "$metadata/mongodb.database") == "$db_name" ]] || fail "Backup MongoDB database name differs from the target."
    (cd "$snapshot_dir" && sha256sum --check "$metadata/snapshot.sha256")
    for folder in kuiper/files/files kuiper/files/raw; do
        [[ ! -d $folder || -z $(find "$folder" -mindepth 1 -print -quit) ]] ||
            fail "The target evidence directory must be empty: $folder"
    done
    collections=$(mongo mongo --quiet "$db_name" --eval 'print(db.getCollectionNames().length)')
    [[ $collections == 0 ]] || fail "The target MongoDB database must be empty."
    es_path='_cat/indices?format=json&expand_wildcards=all' es |
        jq -e 'all(.[]; .index | startswith("."))' >/dev/null || fail "The target Elasticsearch cluster already has case indices."

    register true
    es_path="_snapshot/$repository/kuiper/_restore?wait_for_completion=true" \
        es -X POST -d '{"include_global_state":false}' |
        jq -e '.snapshot.shards.failed == 0' >/dev/null
    unregister
    reclose=$(closed_indices)
    close_indices
    mongo mongorestore --archive < "$metadata/mongodb.archive"
    mkdir -p kuiper/files
    tar -C kuiper/files -xf "$metadata/evidence.tar"
fi

printf 'Kuiper %s completed for %s\n' "$action" "$id"
