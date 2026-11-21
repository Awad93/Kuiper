#!/usr/bin/env bash
set -euo pipefail

case_id=ci_ingestion
machine="${case_id}_good"
app=http://127.0.0.1:5000
es=http://127.0.0.1:9200
record='{"Data":{"@timestamp":"2020-01-01T00:00:00Z","marker":"kuiper_smoke"},"data_type":"ci_fixture"}'
http() { curl -fsS --connect-timeout 5 --max-time 30 "$@"; }

progress() {
    http -H 'Content-Type: text/plain' --data-binary "{\"machines_list\":[\"$machine\"]}" \
        "$app/case/$case_id/progress" |
        jq -e '.[0].progress | .done == 1 and .error == 1 and .queued == 0 and .parsing == 0' >/dev/null
}

if [[ ${1:-} == seed ]]; then
    tmp=$(mktemp -d)
    trap 'rm -r -- "$tmp"' EXIT
    printf '%s\n%s\n' "$record" "$record" > "$tmp/good.kjson"
    printf 'malformed json\n' > "$tmp/bad.kjson"

    http -d "casename=$case_id&status=active&update_or_create_case=create" \
        "$app/admin/create_case" >/dev/null
    http --head "$es/$case_id" >/dev/null
    http -d 'casename=ci_inactive&status=active&update_or_create_case=create' \
        "$app/admin/create_case" >/dev/null
    http -d 'casename=ci_inactive&status=not_active&update_or_create_case=update' \
        "$app/admin/create_case" >/dev/null
    http -F "files[]=@$tmp/good.kjson" "$app/case/$case_id/upload_machine" |
        jq -e '.result == true' >/dev/null
    http -F "files[]=@$tmp/bad.kjson" "$app/case/$case_id/uploadartifacts/$machine" |
        jq -e '.result == true' >/dev/null
    http "$app/case/$case_id/processartifacts/$machine/kjson" |
        jq -e '.data == "started processing"' >/dev/null
    for attempt in {1..60}; do
        if progress; then break; fi
        sleep 2
    done
fi

progress
http "$es/_cat/indices/ci_inactive?format=json&expand_wildcards=all&h=status" |
    jq -e '.[0].status == "close"' >/dev/null
http -X POST "$es/$case_id/_refresh" >/dev/null
http "$es/$case_id/_search?size=3" |
    jq -e --arg machine "$machine" '
        .hits.total.value == 2 and
        all(.hits.hits[]; ._source.machine == $machine and ._source.Data.marker == "kuiper_smoke") and
        ([.hits.hits[]._id] | unique | length == 2)
    ' >/dev/null

processed=(kuiper/files/files/"$case_id"/"$machine"/*/good.kjson)
raw=(kuiper/files/raw/"$case_id"/"$machine"/*-good.kjson)
[[ ${#raw[@]} == 1 && ${#processed[@]} == 1 ]]
cmp "${processed[0]}" "${raw[0]}"
printf '%s\n%s\n' "$record" "$record" | cmp - "${raw[0]}"
http "$app/case/$case_id/machine_files/$machine" | grep -F 'good.kjson' >/dev/null
