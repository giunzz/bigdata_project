"""Validate completed real runs, aggregate all repetitions, and draw figures."""
from pathlib import Path
from collections import defaultdict
import argparse, csv, hashlib, json, math, os, statistics
from benchmark import summarize, percentile

ROOT = Path(__file__).resolve().parent

def lines(path):
    with path.open(encoding='utf-8') as src:
        for n, line in enumerate(src, 1):
            if line.strip():
                try: yield json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f'{path.name}, line {n}: {error}') from error

def save_csv(path, rows):
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='', encoding='utf-8-sig') as out:
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)

def avg_sd(rows, key):
    values = [float(row[key]) for row in rows]
    return statistics.mean(values), statistics.stdev(values) if len(values) > 1 else 0.0

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, default=ROOT/'results')
    args = parser.parse_args(); out = args.results.resolve()
    config = json.loads((out/'experiment.json').read_text())
    complete = json.loads((out/'completed.json').read_text())
    if complete['status'] != 'succeeded': raise ValueError('Measurement did not succeed.')
    assert hashlib.sha256((out/'receipts.jsonl').read_bytes()).hexdigest() == complete['canonical_receipts']['sha256']
    for item in lines(out/'receipt_manifests.jsonl'):
        path = out/item['file']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
        assert sum(1 for _ in path.open('rb')) == item['events']
    rows = summarize(out/'runs.csv', out/'progress.jsonl', out/'receipts.jsonl')
    expected_runs = len(config['rates'])*config['repeats']
    assert len(rows) == complete['measured_runs'] == expected_runs
    run_ids = {row['run_id'] for row in rows}
    by_rate = defaultdict(list)
    for row in rows: by_rate[int(row['target_rate_mps'])].append(row)
    progress = defaultdict(dict); stages = defaultdict(dict); samples = defaultdict(list)
    for item in lines(out/'progress.jsonl'):
        rid = item.get('benchmark_run_id')
        if rid in run_ids: progress[rid][(item['id'], item['batchId'])] = item
    for item in lines(out/'batch_stages.jsonl'):
        if item['run_id'] in run_ids: stages[item['run_id']][item['batch_id']] = item
    for item in lines(out/'offset_resources.jsonl'):
        if item['run_id'] in run_ids: samples[item['run_id']].append(item)
    receipts = defaultdict(dict)
    all_receipt_count = 0
    for item in lines(out/'receipts.jsonl'):
        all_receipt_count += 1
        if item['run_id'] not in run_ids: continue
        assert item['ready_at_ms'] >= item['produced_at_ms']
        old = receipts[item['run_id']].get(item['event_id'])
        if old is None or item['ready_at_ms'] < old['ready_at_ms']:
            receipts[item['run_id']][item['event_id']] = item
    checks = []
    for row in rows:
        rid = row['run_id']; n = int(row['attempted_messages'])
        input_rows = sum(b['numInputRows'] for b in progress[rid].values())
        stage_input = sum(b['input_rows'] for b in stages[rid].values())
        scored = sum(b['scored_rows'] for b in stages[rid].values())
        invalid = sum(b['invalid_or_duplicate_rows'] for b in stages[rid].values())
        values = [int(row['sent_messages']), int(row['mongo_verified_count']),
                  int(row['unique_delivered_events']), len(receipts[rid]),
                  input_rows, stage_input, scored]
        assert all(value == n for value in values), (rid, n, values)
        assert invalid == row['invalid_receipt_rows'] == 0
        assert int(row['backlog_after_drain_messages']) == 0
        assert len({r['endpoint'] for r in receipts[rid].values()}) == 1
        peak_lag = max(s['backlog_messages'] for s in samples[rid])
        row['backlog_peak_sampled_messages'] = peak_lag
        checks.append({'run_id': rid, 'expected_events': n, 'producer_acked': values[0],
                       'mongo_count_verified_during_run': values[1], 'unique_receipts': values[3],
                       'spark_input_rows': input_rows, 'scored_rows': scored,
                       'invalid_rows': invalid, 'backlog_after_drain': 0, 'status': 'passed'})
    reference_checks = list(lines(out/'verification.jsonl'))
    assert reference_checks and all(c['status'] == 'passed' for c in reference_checks)
    assert sum(int(r['sent_messages']) for r in rows) == complete['measured_events']
    all_samples = list(lines(out/'offset_resources.jsonl'))
    final_offsets = next(s for s in reversed(all_samples) if s['marker'] == 'after_delivery')
    total_events = complete['measured_events'] + complete['warmup_events']
    assert sum(final_offsets['latest_next_offsets'].values()) == total_events
    assert sum(final_offsets['completed_next_offsets'].values()) == total_events
    assert all_receipt_count == total_events, 'Unexpected retries: inspect receipts before publishing.'
    grouped = []; resource_rows = []; stage_rows = []
    for rate in sorted(by_rate):
        run_rows = by_rate[rate]
        assert len(run_rows) == config['repeats']
        latencies = [r['ready_at_ms']-r['produced_at_ms']
                     for row in run_rows for r in receipts[row['run_id']].values()]
        record = {'target_rate_mps': rate, 'repeats': len(run_rows),
                  'measured_events': len(latencies), 'send_success_pct': 100.0,
                  'latency_ms_mean': statistics.mean(latencies),
                  'latency_ms_p50': percentile(latencies, .50),
                  'latency_ms_p95': percentile(latencies, .95),
                  'latency_ms_p99': percentile(latencies, .99),
                  'latency_ms_max': max(latencies), 'invalid_receipt_rows': 0,
                  'backlog_after_drain_messages': 0}
        for key in ['producer_mps', 'delivered_events_per_second', 'spark_processing_capacity_rps',
                    'drain_seconds', 'backlog_end_messages', 'backlog_peak_sampled_messages',
                    'batch_duration_ms_mean', 'batch_duration_ms_p95']:
            mean, sd = avg_sd(run_rows, key)
            record[key+'_mean'] = mean; record[key+'_sd'] = sd
            record[key+'_min'] = min(float(r[key]) for r in run_rows)
            record[key+'_max'] = max(float(r[key]) for r in run_rows)
        stage_list = [b for row in run_rows for b in stages[row['run_id']].values()]
        stage_record = {'target_rate_mps': rate, 'nonempty_batches': len(stage_list)}
        for key in ['input_materialize_ms', 'score_collect_ms', 'mongo_write_ms',
                    'receipt_write_ms', 'callback_ms']:
            stage_record[key+'_mean'] = statistics.mean(b[key] for b in stage_list)
        stage_record['other_ms_mean'] = stage_record['callback_ms_mean']-sum(
            stage_record[key+'_mean'] for key in ['input_materialize_ms', 'score_collect_ms',
                                                'mongo_write_ms', 'receipt_write_ms'])
        stage_rows.append(stage_record)
        rate_samples = [s for row in run_rows for s in samples[row['run_id']]]
        for name in sorted(set(n for s in rate_samples for n in s['processes'])):
            records = [s['processes'][name] for s in rate_samples if 'rss_bytes' in s['processes'].get(name, {})]
            resource_rows.append({'target_rate_mps': rate, 'process': name,
                                  'peak_rss_MiB': max(r['rss_bytes'] for r in records)/2**20,
                                  'peak_sampled_cpu_percent': max(r['cpu_percent'] for r in records),
                                  'scope': 'named process only; not entire system or child workers'})
        grouped.append(record)
    save_csv(out/'benchmark_summary.csv', rows)
    save_csv(out/'benchmark_by_rate.csv', grouped)
    save_csv(out/'batch_stage_summary.csv', stage_rows)
    save_csv(out/'resource_peaks.csv', resource_rows)
    verification = {'status': 'passed', 'experiment_id': config['experiment_id'],
                    'measured_runs': len(rows), 'measured_events': complete['measured_events'],
                    'warmup_events_excluded': complete['warmup_events'],
                    'final_kafka_offsets': final_offsets['latest_next_offsets'],
                    'final_completed_spark_offsets': final_offsets['completed_next_offsets'],
                    'offline_online_reference_checks': reference_checks, 'run_checks': checks,
                    'latency_aggregation': 'pooled unique events per rate; linear interpolated quantiles',
                    'rate_aggregation': 'arithmetic mean and sample standard deviation of 3 full run rates',
                    'observation_window': 'first scheduled send until last successful MongoDB journal ACK',
                    'measurement_scope': config['scope']}
    verification['canonical_receipt_export'] = complete['canonical_receipts']
    (out/'analysis_verification.json').write_text(json.dumps(verification, ensure_ascii=False, indent=2))
    alert_example = json.loads((out/'mongo_alert_examples.json').read_text())[0]
    receipt = receipts[alert_example['run_id']][alert_example['event_id']]
    trace = {'mongo_document': alert_example, 'receipt': receipt,
             'batch_stages': stages[alert_example['run_id']][alert_example['batch_id']],
             'spark_progress': next(b for b in progress[alert_example['run_id']].values()
                                    if b['batchId'] == alert_example['batch_id']),
             'event_latency_ms': receipt['ready_at_ms']-receipt['produced_at_ms'],
             'producer_ack_evidence': 'run-level ACK count; no per-event partition/offset acknowledgement log',
             'dashboard_evidence': None}
    (out/'event_trace.json').write_text(json.dumps(trace, ensure_ascii=False, indent=2))
    figures(out, grouped, rows, samples, stage_rows)
    conclusion(out, config, complete, grouped, stage_rows)
    print(json.dumps({'verification': 'passed', 'measured_events': complete['measured_events'],
                      'by_rate': grouped}, ensure_ascii=False, indent=2))

def figures(out, grouped, rows, samples, stage_rows):
    os.environ['MPLBACKEND'] = 'Agg'
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 12, 'axes.labelsize':14, 'axes.titlesize':15})
    colors = ['#215A8E', '#C77722', '#328564']; x = np.arange(len(grouped))
    labels = [str(r['target_rate_mps']) for r in grouped]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    ax.bar(x-.25, [r['target_rate_mps'] for r in grouped], .25, color='#B8C0C8', label='Tải mục tiêu')
    ax.bar(x, [r['producer_mps_mean'] for r in grouped], .25, color=colors[0],
           yerr=[r['producer_mps_sd'] for r in grouped], capsize=4, label='Producer ACK')
    ax.bar(x+.25, [r['delivered_events_per_second_mean'] for r in grouped], .25, color=colors[2],
           yerr=[r['delivered_events_per_second_sd'] for r in grouped], capsize=4, label='Đã ghi MongoDB')
    ax.set(xticks=x, xticklabels=labels, xlabel='Tải mục tiêu (message/s)', ylabel='Sự kiện mỗi giây',
           title='Thông lượng theo tải trên pilot một máy')
    ax.legend(); ax.grid(axis='y', alpha=.18); ax.set_axisbelow(True)
    fig.tight_layout(); fig.savefig(out/'throughput.png', dpi=220); plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 4.6))
    for i, p in enumerate(['50', '95', '99']):
        ax.plot(x, [r['latency_ms_p'+p]/1000 for r in grouped], marker='o', color=colors[i], label='p'+p)
    ax.set(xticks=x, xticklabels=labels, xlabel='Tải mục tiêu (message/s)', ylabel='Độ trễ (giây)',
           title='Độ trễ từ Producer đến xác nhận ghi MongoDB')
    ax.legend(); ax.grid(alpha=.2); ax.set_ylim(bottom=0)
    fig.tight_layout(); fig.savefig(out/'latency.png', dpi=220); plt.close(fig)
    fig, axes = plt.subplots(3, 1, figsize=(9, 7.0), sharex=True)
    for ax, group in zip(axes, grouped):
        for row in rows:
            if int(row['target_rate_mps']) != group['target_rate_mps']: continue
            ss = sorted(samples[row['run_id']], key=lambda s: s['sampled_at_ms'])
            ax.plot([(s['sampled_at_ms']-int(row['produced_start_ms']))/1000 for s in ss],
                    [s['backlog_messages'] for s in ss], lw=1.4, label='Lượt '+str(row['repeat']))
        ax.axvline(60, color='#555555', ls='--', lw=1)
        ax.set_ylabel(str(group['target_rate_mps'])+' msg/s\nBacklog'); ax.set_ylim(bottom=0); ax.grid(alpha=.2)
    fig.suptitle('Backlog theo thời gian ở ba lượt lặp mỗi mức tải',fontsize=15,y=.995)
    handles, names = axes[0].get_legend_handles_labels()
    fig.legend(handles,names,loc='upper center',bbox_to_anchor=(.5,.95),ncol=3,fontsize=12)
    axes[-1].set_xlabel('Giây từ lúc bắt đầu phát; vạch đứt ở 60 giây',fontsize=13)
    fig.tight_layout(rect=[0,0,1,.90]); fig.savefig(out/'backlog.png', dpi=220); plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 5.3)); bottom = np.zeros(len(stage_rows))
    for key, label, color in [
            ('input_materialize_ms_mean', 'Đọc và ghép dữ liệu', '#8098B0'),
            ('score_collect_ms_mean', 'Đặc trưng RF và collect', '#215A8E'),
            ('mongo_write_ms_mean', 'Ghi MongoDB có journal', '#328564'),
            ('receipt_write_ms_mean', 'Ghi receipts đo', '#C77722'),
            ('other_ms_mean', 'Phần còn lại callback', '#B8C0C8')]:
        values = np.array([r[key]/1000 for r in stage_rows])
        ax.bar(x, values, bottom=bottom, label=label, color=color); bottom += values
    ax.set(xticks=x, xticklabels=labels, xlabel='Tải mục tiêu (message/s)', ylabel='Giây mỗi callback có dữ liệu',
           title='Thời gian trung bình các phần trong foreachBatch')
    ax.legend(fontsize=11, loc='upper center',bbox_to_anchor=(.5,.98),ncol=2)
    ax.set_ylim(0,max(bottom)*1.52)
    ax.grid(axis='y', alpha=.18); ax.set_axisbelow(True)
    fig.tight_layout(); fig.savefig(out/'batch_stages.png', dpi=220); plt.close(fig)

def conclusion(out, config, complete, grouped, stage_rows):
    content = ['# Kết quả đo Kafka Spark MongoDB của Phúc', '',
               'Đây là phép đo thật trên một pilot độc lập chạy một máy. Chưa có code và cấu hình triển khai của nhóm để đo chính hệ thống đó.', '',
               f"Experiment: `{config['experiment_id']}`. Ngày UTC: {config['execution_date_utc'][:10]}.",
               f"Đã đo {complete['measured_runs']} lượt, {complete['measured_events']:,} sự kiện; loại {complete['warmup_events']:,} sự kiện warm-up.", '',
               '| Tải msg/s | Producer ACK/s trung bình ± SD | Ghi MongoDB/s trung bình ± SD | p95 giây | p99 giây | Backlog cuối phát TB | Drain giây TB |',
               '|---:|---:|---:|---:|---:|---:|---:|']
    for r in grouped:
        content.append(f"| {r['target_rate_mps']} | {r['producer_mps_mean']:.2f} ± {r['producer_mps_sd']:.2f} | "
                       f"{r['delivered_events_per_second_mean']:.2f} ± {r['delivered_events_per_second_sd']:.2f} | "
                       f"{r['latency_ms_p95']/1000:.3f} | {r['latency_ms_p99']/1000:.3f} | "
                       f"{r['backlog_end_messages_mean']:.1f} | {r['drain_seconds_mean']:.3f} |")
    content += ['', 'Tất cả lượt có ACK = số sự kiện dự kiến = số bản ghi MongoDB = số receipt duy nhất = số dòng Spark scoring. Backlog sau drain bằng 0.', '',
                'Throughput đầu ra gồm thời gian chờ trigger và drain. Spark processing capacity chỉ chia cho tổng thời gian trigger có dữ liệu và được ghi riêng trong CSV.',
                'Latency lấy produced_at_ms ngay trước gửi JSON và ready_at_ms sau ACK của MongoDB với w=1, j=true. Hai điểm đo dùng cùng đồng hồ máy. Quantile được gộp mọi event của ba lượt cùng tải và nội suy tuyến tính; SD thông lượng là SD mẫu của ba lượt.',
                'Backlog là Kafka latest next-offset trừ endOffset của batch Spark đã hoàn thành, theo từng partition. Thu mẫu khoảng mỗi giây. Không dùng committed offset của Kafka consumer group.', '',
                'Dữ liệu replay là 6.480 phép đo test BDG2 thật, đọc tuần tự và lặp lại khi hết; mỗi lần phát có event_id mới. Lượt 100 msg/s phát 6.000 sự kiện; các tải cao có nhiều vòng replay hơn.',
                'Weather và metadata là bảng lịch sử tĩnh đã cache; RF và ngưỡng được nạp một lần. Benchmark không gồm huấn luyện, Delta ETL, Airflow, mạng liên máy hoặc dashboard.',
                'File analysis_verification.json chứa kiểm tra toàn vẹn. CSV và JSONL trong results là bằng chứng để tính lại kết quả. Các ví dụ MongoDB là bản ghi đã ghi sink, không phải ảnh giao diện.']
    (out/'KET_QUA_DO.md').write_text('\n'.join(content)+'\n', encoding='utf-8')

if __name__ == '__main__': main()
