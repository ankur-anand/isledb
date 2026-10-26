"""Writes the Grafana dashboard for stress runs:

    python3 deploy/grafana/dashboard.py deploy/grafana/dashboards/stress.json

Edit the panels here and run it again, rather than editing the JSON.
"""
import json, sys

DS = {"type": "prometheus", "uid": "prometheus"}
R = 'run="$run"'
panels, pid, y = [], [1], [0]


def nid():
    pid[0] += 1
    return pid[0]


def row(title):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y[0]}, "panels": []})
    y[0] += 1


x = [0]


def place(w, h):
    if x[0] + w > 24:
        x[0] = 0
        y[0] += place.h
    pos = {"h": h, "w": w, "x": x[0], "y": y[0]}
    x[0] += w
    place.h = h
    return pos


place.h = 0


def newline():
    if x[0]:
        y[0] += place.h
        x[0] = 0


def targets(exprs):
    return [{"datasource": DS, "expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(exprs)]


def ts(title, exprs, unit="short", w=8, h=8, desc="", stack=False, minv=None, right=None):
    """right maps a series' legend to its unit, drawn on a second axis on
    the right: for series whose units or sizes differ from the rest."""
    p = {"type": "timeseries", "title": title, "id": nid(), "datasource": DS, "gridPos": place(w, h),
         "description": desc, "targets": targets(exprs),
         "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10,
                                                               "stacking": {"mode": "normal" if stack else "none"}}},
                         "overrides": [{"matcher": {"id": "byName", "options": name},
                                        "properties": [{"id": "unit", "value": u},
                                                       {"id": "custom.axisPlacement", "value": "right"}]}
                                       for name, u in (right or {}).items()]},
         "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}}
    if minv is not None:
        p["fieldConfig"]["defaults"]["min"] = minv
    panels.append(p)


def stat(title, expr, unit="short", w=3, h=4, desc="", thresholds=None, text="value", legend=""):
    steps = thresholds or [{"color": "green", "value": None}]
    panels.append({"type": "stat", "title": title, "id": nid(), "datasource": DS, "gridPos": place(w, h),
                   "description": desc, "targets": targets([(expr, legend)]),
                   "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}},
                                   "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                               "textMode": text, "colorMode": "value", "graphMode": "none"}})


def text(title, content, w=24, h=3):
    panels.append({"type": "text", "title": title, "id": nid(), "gridPos": place(w, h),
                   "options": {"mode": "markdown", "content": content}})


def q(p, metric, by=""):
    return f'histogram_quantile({p}, sum by (le{", " + by if by else ""}) (rate({metric}{{{R}}}[2m])))'


red_above = lambda v: [{"color": "green", "value": None}, {"color": "red", "value": v}]
amber_red = lambda a, r: [{"color": "green", "value": None}, {"color": "orange", "value": a}, {"color": "red", "value": r}]

row("Run")
stat("Phase", f'stress_phase{{{R}}}', text="name", legend="{{phase}}", w=4)
stat("Written", f'stress_written_bytes_total{{{R}}}', "bytes")
stat("Planned", f'stress_target_bytes{{{R}}}', "bytes")
stat("Rate", f'rate(stress_written_bytes_total{{{R}}}[2m])', "Bps")
stat("Time left", f'(stress_target_bytes{{{R}}} - stress_written_bytes_total{{{R}}}) / rate(stress_written_bytes_total{{{R}}}[5m])', "s",
     desc="At the rate of the last 5 minutes, for the bytes the run plans to write.")
stat("Wrong answers", f'sum(stress_verify_failures_total{{{R}}})', thresholds=red_above(1),
     desc="Reads that failed verification. Any is a failed run: see failures.jsonl.")
stat("Pending checks", f'stress_verify_pending{{{R}}}', thresholds=amber_red(1000, 100000))
stat("Write amp", f'stress_write_amplification{{{R},by="total"}}', "none",
     desc="SST bytes uploaded by the writer and maintenance per key and value byte written, first save included.")
newline()
stat("Space amp", f'stress_space_amplification{{{R}}}', "none", w=4,
     desc="SST bytes in the bucket per byte the database holds, from the last listing.")
stat("Disk used", f'stress_disk_used_ratio{{{R}}}', "percentunit", thresholds=amber_red(0.8, 0.85))
stat("Writes held", f'stress_disk_held{{{R}}}', thresholds=red_above(1),
     desc="1 while the disk guard holds new writes because the disk is too full.")
stat("Held for", f'stress_disk_held_seconds_total{{{R}}}', "s")
stat("Refused/s", f'rate(stress_writes_total{{{R},outcome="refused"}}[5m])', "short", thresholds=amber_red(0.1, 1))
stat("Unresolved", f'stress_writes_total{{{R},outcome="unresolved"}}', thresholds=amber_red(1, 10))
stat("Firing alerts", 'ALERTS{alertstate="firing"}', w=5, text="name", legend="{{alertname}}", thresholds=red_above(1),
     desc="Alerts firing now; Prometheus lists them all at /alerts.")
panels[-1]["fieldConfig"]["defaults"]["noValue"] = "none"
newline()

row("Writes")
ts("Written per second", [(f'rate(stress_written_bytes_total{{{R}}}[1m])', "written")], "Bps")
ts("Write answers per second", [(f'sum by (outcome) (rate(stress_writes_total{{{R}}}[1m]))', "{{outcome}}")], "short",
   desc="committed; refused (503), nothing written; not_committed, never saved; unknown, asked about; unresolved, sent again.")
ts("Commit latency, as the client sees it",
   [(q(0.5, "stress_commit_seconds_bucket"), "p50"), (q(0.95, "stress_commit_seconds_bucket"), "p95"),
    (q(0.99, "stress_commit_seconds_bucket"), "p99")], "s")
newline()
ts("Writer save time", [(q(0.5, "isledb_writer_commit_seconds_bucket"), "p50"), (q(0.99, "isledb_writer_commit_seconds_bucket"), "p99")], "s",
   desc="How long the writer takes to save a group of writes to storage.")
ts("Writer: waiting to be saved", [(f'isledb_writer_pending_memtables{{{R}}}', "groups waiting"),
                                   (f'isledb_writer_pending_bytes{{{R}}}', "bytes waiting")], "short",
   right={"bytes waiting": "bytes"})
ts("Writer merges", [(f'rate(isledb_writer_l0_merge_bytes_total{{{R}}}[2m])', "merged"),
                     (f'isledb_writer_l0_merge_held_bytes{{{R}}}', "held in memory")], "Bps",
   right={"held in memory": "bytes"})
newline()

row("Reads")
ts("Read latency, as the client sees it",
   [(q(0.5, "stress_read_seconds_bucket", "op"), "{{op}} p50"), (q(0.99, "stress_read_seconds_bucket", "op"), "{{op}} p99")], "s")
ts("Reads per second", [(f'sum by (op, result) (rate(stress_reads_total{{{R}}}[1m]))', "{{op}} {{result}}")], "short")
ts("Age of the data readers serve", [(f'time() - isledb_reader_view_loaded_timestamp_seconds{{{R}}}', "{{role}}")], "s")
newline()

row("Correctness")
ts("Reads checked per second", [(f'rate(stress_verify_checked_total{{{R}}}[1m])', "checked")], "short")
ts("Checks waiting for their revision to settle", [(f'stress_verify_pending{{{R}}}', "pending")], "short")
ts("Wrong answers by kind", [(f'stress_verify_failures_total{{{R}}} > 0', "{{kind}}")], "short", minv=0)
newline()

row("Compaction")
ts("Compaction bytes per second", [(f'rate(isledb_maintenance_compaction_rewritten_bytes_total{{{R}}}[2m])', "written"),
                                    (f'rate(isledb_maintenance_compaction_read_bytes_total{{{R}}}[2m])', "read"),
                                    (f'rate(isledb_maintenance_compaction_moved_bytes_total{{{R}}}[2m])', "moved")], "Bps")
ts("Compaction jobs per minute", [(f'increase(isledb_maintenance_compaction_jobs_total{{{R}}}[1m])', "jobs"),
                                   (f'increase(isledb_maintenance_compaction_oversized_jobs_total{{{R}}}[1m])', "oversized"),
                                   (f'increase(isledb_maintenance_commands_rejected_total{{{R}}}[1m])', "refused by the writer")], "short")
ts("Finished work waiting for the writer",
   [(f'(time() - isledb_maintenance_waiting_for_writer_since_timestamp_seconds{{{R}}}) * (isledb_maintenance_waiting_for_writer_since_timestamp_seconds{{{R}}} > bool 0)', "waiting")], "s",
   desc="How long the oldest finished compaction has waited for the writer to publish it; 0 when nothing waits.")
newline()
ts("L0 files and depth", [(f'isledb_maintenance_l0_files{{{R}}}', "files"), (f'isledb_maintenance_l0_depth{{{R}}}', "depth")], "short",
   desc="New files not yet merged into L1, and the most of them one key is in. Growing without stopping: compaction can't keep up. "
        "Empty for versions that don't export it.")
ts("L0 bytes", [(f'isledb_maintenance_l0_bytes{{{R}}}', "L0")], "bytes")
ts("Compaction urgent", [(f'isledb_maintenance_compaction_critical{{{R}}}', "urgent")], "short", minv=0,
   desc="1 while the compaction running is urgent because L0 is too deep.")
newline()
ts("Level bytes against target", [(f'isledb_maintenance_level_bytes{{{R}}}', "L{{level}}"),
                                   (f'isledb_maintenance_level_target_bytes{{{R}}}', "L{{level}} target")], "bytes")
ts("Level fill (bytes / target)", [(f'isledb_maintenance_level_bytes{{{R}}} / isledb_maintenance_level_target_bytes{{{R}}}', "L{{level}}")], "percentunit",
   desc="Above 100% the level waits to be merged down; far above, every merge into it costs more.")
ts("Compaction time, by kind", [(q(0.5, "isledb_maintenance_compaction_seconds_bucket", "kind"), "{{kind}} p50"),
                                 (q(0.99, "isledb_maintenance_compaction_seconds_bucket", "kind"), "{{kind}} p99")], "s",
   desc="Jobs hold the single compaction slot for this long.")
newline()

row("Storage")
ts("Uploaded per second, by proxy and kind",
   [(f'sum by (proxy, kind) (rate(stress_storage_uploaded_bytes_total{{{R},method="PUT"}}[1m]))', "{{proxy}} {{kind}}")], "Bps", stack=True)
ts("Requests per second, by proxy and method",
   [(f'sum by (proxy, method) (rate(stress_storage_requests_total{{{R}}}[1m]))', "{{proxy}} {{method}}")], "short")
ts("Storage errors per second",
   [(f'sum by (proxy) (rate(stress_storage_server_errors_total{{{R}}}[1m]))', "{{proxy}} 5xx"),
    (f'sum by (proxy) (rate(stress_storage_injected_total{{{R}}}[1m]))', "{{proxy}} injected"),
    (f'sum by (proxy) (rate(stress_storage_dropped_total{{{R}}}[1m]))', "{{proxy}} dropped")], "short")
newline()
ts("Write amplification", [(f'stress_write_amplification{{{R}}}', "{{by}}")], "none")
ts("Space amplification", [(f'stress_space_amplification{{{R}}}', "space")], "none")
ts("Bucket bytes, by kind", [(f'stress_bucket_bytes{{{R}}}', "{{kind}}")], "bytes", stack=True)
newline()

row("SILO")
ts("SILO CPU (cores)", [('rate(minio_process_cpu_seconds_total{job="silo"}[1m])', "SILO")], "short",
   desc="Compare with SILO_CPUS: near it, SILO is the bottleneck.")
ts("SILO requests per second, by call", [('sum by (api) (rate(minio_s3_requests_total{job="silo"}[1m]))', "{{api}}")], "short")
ts("SILO requests in flight and waiting", [('sum(minio_s3_requests_inflight_total{job="silo"})', "in flight"),
                                           ('sum(minio_s3_requests_waiting_total{job="silo"})', "waiting")], "short")
newline()
ts("SILO traffic", [('sum(rate(minio_s3_traffic_received_bytes{job="silo"}[1m]))', "received"),
                    ('sum(rate(minio_s3_traffic_sent_bytes{job="silo"}[1m]))', "sent")], "Bps")
ts("SILO drive", [('max(minio_node_drive_perc_util{job="silo-resource"})', "busy %"),
                  ('max(minio_node_drive_writes_await{job="silo-resource"})', "write await ms"),
                  ('max(minio_node_drive_reads_await{job="silo-resource"})', "read await ms")], "percent",
   right={"write await ms": "ms", "read await ms": "ms"})
ts("SILO drive space", [('sum(minio_node_drive_used_bytes{job="silo"})', "used"), ('sum(minio_node_drive_free_bytes{job="silo"})', "free")], "bytes")
newline()

row("Processes and host")
ts("CPU by process (cores)", [(f'rate(process_cpu_seconds_total{{{R}}}[1m])', "{{role}}"),
                              ('rate(minio_process_cpu_seconds_total{job="silo"}[1m])', "silo")], "short", stack=True)
ts("Memory by process", [(f'process_resident_memory_bytes{{{R}}}', "{{role}}"),
                         ('minio_process_resident_memory_bytes{job="silo"}', "silo")], "bytes")
ts("Goroutines by process", [(f'go_goroutines{{{R}}}', "{{role}}")], "short")
newline()
ts("Host CPU", [('max(minio_node_cpu_avg_user{job="silo-resource"})', "user %"),
                ('max(minio_node_cpu_avg_system{job="silo-resource"})', "system %"),
                ('max(minio_node_cpu_avg_iowait{job="silo-resource"})', "iowait %")], "short",
   desc="The whole host, as SILO sees it.")
ts("Host memory", [('max(minio_node_mem_used{job="silo-resource"})', "used"), ('max(minio_node_mem_cache{job="silo-resource"})', "cache"),
                   ('max(minio_node_mem_available{job="silo-resource"})', "available")], "bytes")
ts("Disk under the run", [(f'stress_disk_used_ratio{{{R}}}', "used"), (f'stress_disk_held{{{R}}}', "writes held")], "percentunit", minv=0,
   right={"writes held": "short"})
newline()

dashboard = {
    "uid": "isledb-stress", "title": "isledb stress", "schemaVersion": 39, "version": 1, "editable": False,
    "time": {"from": "now-1h", "to": "now"}, "refresh": "30s", "timezone": "utc", "tags": ["isledb", "stress"],
    "templating": {"list": [{
        "name": "run", "label": "Run", "type": "query", "datasource": DS,
        "query": {"query": "label_values(stress_run_info, run)", "refId": "run"},
        "definition": "label_values(stress_run_info, run)", "refresh": 2, "sort": 2, "includeAll": False, "multi": False,
    }]},
    "annotations": {"list": []},
    "panels": panels,
}
json.dump(dashboard, open(sys.argv[1], "w"), indent=1)
print(len(panels), "panels")
