# Example usage

This example demonstrates expected behavior for `user:alice`.

## 1. Prepare source metrics

```bash
mkdir -p /data/metrics
cp /Users/henneberger/libs/metricfs/examples/metrics/orders.jsonl /data/metrics/orders.jsonl
cp /Users/henneberger/libs/metricfs/examples/metrics/.metricfs-map.yaml /data/metrics/.metricfs-map.yaml
mkdir -p /data/metrics/openlineage
cp /Users/henneberger/libs/metricfs/examples/metrics/openlineage/.metricfs-map.yaml /data/metrics/openlineage/.metricfs-map.yaml
mkdir -p /mnt/metrics-alice
```

## 2. Start SpiceDB and load schema

```bash
docker run -d --name metricfs-spicedb-test \
  -p 50051:50051 -p 8443:8443 \
  authzed/spicedb serve-testing --http-enabled

docker run --rm -v "/Users/henneberger/libs/examples:/examples:ro" authzed/zed:latest \
  schema write /examples/spicedb-schema.zed \
  --endpoint host.docker.internal:50051 \
  --token testtoken \
  --insecure
```

The sample relationships include namespace inheritance and a transitive chain
through orb and org membership.

Transitive example included in the sample:

- `user:alice -> orb:data_eng -> org:acme -> namespace:acme -> metric_row:orders_1`
- Alice can read `orders_1` and `orders_3` via this transitive path (without
  direct per-row grants).

Load relationship tuples:

```bash
while IFS= read -r line; do
  line="${line%$'\r'}"
  [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
  resource_part="${line%%@*}"; subject_part="${line#*@}"
  resource="${resource_part%%#*}"; relation="${resource_part#*#}"
  res_type="${resource%%:*}"; res_id="${resource#*:}"
  subject_obj="${subject_part%%#*}"; subj_rel=""
  [[ "$subject_part" == *"#"* ]] && subj_rel="${subject_part#*#}"
  sub_type="${subject_obj%%:*}"; sub_id="${subject_obj#*:}"
  payload=$(jq -nc --arg rt "$res_type" --arg rid "$res_id" --arg rel "$relation" --arg st "$sub_type" --arg sid "$sub_id" --arg srel "$subj_rel" \
    '{updates:[{operation:"OPERATION_TOUCH",relationship:{resource:{objectType:$rt,objectId:$rid},relation:$rel,subject:{object:{objectType:$st,objectId:$sid}}}}]} | if $srel != "" then .updates[0].relationship.subject.optionalRelation = $srel else . end')
  curl -sS -f -X POST 'http://127.0.0.1:8443/v1/relationships/write' \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer testtoken' \
    --data "$payload" >/dev/null
done < /Users/henneberger/libs/examples/relationships.zed
```

## 3. Mount filtered view for alice

```bash
metricfs mount \
  --source-dir /data/metrics \
  --mount-dir /mnt/metrics-alice \
  --auth-backend spicedb \
  --subject user:alice \
  --spicedb-endpoint http://127.0.0.1:8443 \
  --spicedb-token testtoken \
  --read-only \
  --allow-other=false \
  --mapper-file-name .metricfs-map.yaml \
  --mapper-resolution nearest_ancestor \
  --mapper-inherit-parent \
  --missing-mapper deny \
  --missing-resource-key deny
```

`metricfs` auto-discovers mapping rules from per-directory `.metricfs-map.yaml`
files. No global dataset list is required.

## 4. Query with normal shell tools

```bash
cat /mnt/metrics-alice/orders.jsonl
```

Expected output:

```json
{"metric_row_id":"orders_1","tenant":"acme","metric":"latency_ms","value":112}
{"metric_row_id":"orders_3","tenant":"acme","metric":"error_rate","value":0.021}
```

Regular tooling works as-is:

```bash
awk -F'"' '/tenant/ {print $8}' /mnt/metrics-alice/orders.jsonl
grep '"metric":"error_rate"' /mnt/metrics-alice/orders.jsonl
jq -c '. | select(.value > 0.02)' /mnt/metrics-alice/orders.jsonl
```

## 5. Change policy and see update quickly

Grant alice access to one more row:

```bash
zed relationship create metric_row:orders_4#viewer@user:alice
```

New opens should reflect the policy update within seconds:

```bash
cat /mnt/metrics-alice/orders.jsonl
```

Expected output after update:

```json
{"metric_row_id":"orders_1","tenant":"acme","metric":"latency_ms","value":112}
{"metric_row_id":"orders_3","tenant":"acme","metric":"error_rate","value":0.021}
{"metric_row_id":"orders_4","tenant":"delta","metric":"latency_ms","value":143}
```

## 6. OpenLineage mapping model

For OpenLineage-style rows, place a directory mapper file at
`/data/metrics/openlineage/.metricfs-map.yaml` that emits a canonical job key
per event:

- Job object from `/event/job/{namespace,name}`

The sample mapper uses `decision: any`, so the line is visible if the subject
has `read` on the emitted job object. It also includes fallback paths for job
fields in facets when top-level fields are missing.

Use namespace inheritance in SpiceDB to avoid per-job grants.
