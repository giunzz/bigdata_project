"""Measured BDG2 replay: Kafka -> Spark enrichment/RF/alerts -> journaled MongoDB.

The receiver is a single-node pilot, not the group's unseen deployment.
Every rate has a separate warm-up phase; all event IDs remain unique on replay.
"""
from pathlib import Path
from collections import defaultdict
import argparse,csv,hashlib,json,math,os,platform,statistics,subprocess,sys,threading,time,uuid
from datetime import datetime,timezone
import psutil
from confluent_kafka import Producer,TopicPartition
from confluent_kafka.admin import AdminClient,NewTopic,OffsetSpec
from pymongo import MongoClient,UpdateOne
from pymongo.write_concern import WriteConcern
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark.sql.streaming import StreamingQueryListener
from phuc_analytics import AnalyticsBundle
from process_metrics import metric_pid

ROOT=Path(__file__).resolve().parent

def append(path,data):
    with path.open('a',encoding='utf-8') as out:out.write(json.dumps(data,ensure_ascii=False,allow_nan=False)+'\n')

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',type=Path,default=ROOT/'runtime');p.add_argument('--gold',type=Path,default=ROOT/'data/bdg2_gold.csv')
    p.add_argument('--test',type=Path,default=ROOT/'data/predictions_test_full.csv');p.add_argument('--model',type=Path,default=ROOT/'model')
    p.add_argument('--output',type=Path,default=ROOT/'results');p.add_argument('--rates',default='100,500,1000');p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--seconds',type=int,default=60);p.add_argument('--warmup',type=int,default=30);p.add_argument('--trigger',type=int,default=5)
    p.add_argument('--partitions',type=int,default=3);p.add_argument('--drain-timeout',type=int,default=300);a=p.parse_args()
    a.output=a.output.resolve();a.output.mkdir(parents=True,exist_ok=True)
    if (a.output/'experiment.json').exists():raise RuntimeError('Use a new --output directory; existing measurements are never overwritten.')
    exp='phuc_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:6]
    topic=exp;out=a.output;bootstrap='127.0.0.1:19092'
    services=json.loads((a.runtime/'services.json').read_text())
    if services.get('java_home'):os.environ['JAVA_HOME']=services['java_home']
    os.environ['MPLBACKEND']='Agg';os.environ['SPARK_LOCAL_IP']='127.0.0.1'
    os.environ['PYSPARK_PYTHON']=sys.executable
    admin=AdminClient({'bootstrap.servers':bootstrap})
    for future in admin.create_topics([NewTopic(topic,num_partitions=a.partitions,replication_factor=1)]).values():future.result(timeout=30)
    mongo=MongoClient(services['mongo_uri'],serverSelectionTimeoutMS=5000,connectTimeoutMS=5000,socketTimeoutMS=60000)
    collection=mongo['phuc_measured_pilot'][exp].with_options(write_concern=WriteConcern(w=1,j=True))
    collection.create_index('run_id');collection.create_index([('run_id',1),('is_anomaly',1)])
    jars=','.join(str(x.resolve()) for x in sorted((a.runtime/'jars').glob('*.jar')))
    spark=(SparkSession.builder.master('local[2]').appName(exp).config('spark.jars',jars)
           .config('spark.driver.memory','2g').config('spark.sql.session.timeZone','UTC')
           .config('spark.sql.shuffle.partitions','4').config('spark.sql.adaptive.enabled','false')
           .config('spark.ui.showConsoleProgress','false').config('spark.sql.streaming.numRecentProgressUpdates','1000')
           .config('spark.sql.streaming.noDataMicroBatches.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    bundle=AnalyticsBundle(a.model)
    gold=spark.read.option('header',True).csv(str(a.gold.resolve()))
    meta=gold.select('building_id','site_id','primary_use','square_feet','year_built','floor_count').dropDuplicates(['building_id']).cache();meta.count()
    weather_cols=['air_temperature','dew_temperature','cloud_coverage','sea_level_pressure','wind_speed']
    weather=gold.select('site_id','timestamp',*weather_cols).dropDuplicates(['site_id','timestamp']).cache();weather.count()
    with a.test.open(encoding='utf-8-sig',newline='') as src:energy=list(csv.DictReader(src))
    reference={(r['building_id'],r['timestamp']):float(r['prediction']) for r in energy}
    energy=[{'building_id':r['building_id'],'timestamp':r['timestamp'],'meter':0,'meter_reading':float(r['meter_reading'])} for r in energy]
    java=subprocess.check_output([str(Path(services.get('java_home','/usr/lib/jvm/java-17-openjdk-amd64'))/'bin/java'),'-version'],stderr=subprocess.STDOUT,text=True).splitlines()[0]
    configuration={'experiment_id':exp,'execution_date_utc':datetime.now(timezone.utc).isoformat(),'scope':'independent single-node Kafka-Spark-MongoDB pilot; not group deployment or dashboard latency',
        'path':['BDG2 Producer','Kafka','Spark parse + static metadata/weather joins','saved RF and residual detector','MongoDB journal acknowledgement'],
        'source_origin':'real BDG2 RAW','replay_source_rows':len(energy),'unique_event_id_on_each_replay':True,
        'input_gold':str(a.gold),'model':str(a.model),'rates':list(map(int,a.rates.split(','))),'repeats':a.repeats,'send_seconds_target':a.seconds,'warmup_seconds_target':a.warmup,
        'kafka_version':'3.9.1','mongo_version':mongo.server_info()['version'],'spark_version':spark.version,'python_version':platform.python_version(),'java':java,
        'os':platform.platform(),'cpu_visible':psutil.cpu_count(),'cpu_cgroup_quota':Path('/sys/fs/cgroup/cpu.max').read_text().strip() if Path('/sys/fs/cgroup/cpu.max').exists() else None,
        'memory_cgroup_bytes':Path('/sys/fs/cgroup/memory.max').read_text().strip() if Path('/sys/fs/cgroup/memory.max').exists() else None,
        'host_memory_bytes':psutil.virtual_memory().total,'spark_master':'local[2]','spark_driver_memory':spark.sparkContext.getConf().get('spark.driver.memory'),'shuffle_partitions':4,
        'topic':topic,'partitions':a.partitions,'replication_factor':1,'trigger_seconds':a.trigger,'maxOffsetsPerTrigger':None,
        'producer_config':{'acks':'all','enable.idempotence':True,'linger.ms':5,'batch.size':65536,'compression.type':'none'},
        'mongo_write_concern':{'w':1,'j':True},'mongo_chunk_documents':500,'mongo_database':'phuc_measured_pilot','mongo_collection':exp,
        'latency_endpoint':'MongoDB acknowledgement with j=true after upsert; same host wall clock',
        'backlog_method':'latest Kafka next offsets minus completed Spark batch end offsets per partition; sampled every 1 s',
        'deduplication':'unique source event_id; per-batch dedup and MongoDB _id upsert on retry',
        'receipt_export':'freeze actual ACK timestamps from memory after each drained run; fsync and SHA-256 verify per-run files; combine at completion',
        'excluded_from_path':['Delta Lake historical ETL','Airflow retraining','dashboard/API rendering','network between physical hosts']}
    (out/'experiment.json').write_text(json.dumps(configuration,ensure_ascii=False,indent=2))
    lock=threading.RLock();state={'phase':'initializing','completed':{str(i):0 for i in range(a.partitions)},'batch_runs':{},'deliveries':defaultdict(dict),'fatal':None}
    progress_file=out/'progress.jsonl';receipt_file=out/'receipts_live_diagnostic.jsonl';batch_file=out/'batch_stages.jsonl';sample_file=out/'offset_resources.jsonl'
    receipt_sets=out/'receipts_by_run';receipt_sets.mkdir()
    receipt_manifests=[]
    def persist_receipts(rid,n):
        # Freeze actual ACK timestamps already captured during the run. Export
        # after the timing window, rather than relying on a growing append file.
        with lock:records=list(state['deliveries'][rid].values())
        if len(records)!=n:raise RuntimeError('Incomplete in-memory ACK receipts.')
        path=receipt_sets/(rid+'.jsonl');digest=hashlib.sha256()
        with path.open('xb') as dst:
            for record in records:
                encoded=(json.dumps(record)+'\n').encode('utf-8')
                if dst.write(encoded)!=len(encoded):raise RuntimeError('Short receipt write.')
                digest.update(encoded)
            dst.flush();os.fsync(dst.fileno())
        disk_digest=hashlib.sha256(path.read_bytes()).hexdigest()
        if disk_digest!=digest.hexdigest():raise RuntimeError('Receipt export checksum mismatch.')
        disk_count=sum(1 for _ in path.open('rb'))
        if disk_count!=n:raise RuntimeError(f'Receipt export count {disk_count} != {n}')
        receipt_manifests.append({'run_id':rid,'file':str(path.relative_to(out)),'events':n,'sha256':disk_digest})
        append(out/'receipt_manifests.jsonl',receipt_manifests[-1])
    def combine_receipts():
        path=out/'receipts.jsonl';digest=hashlib.sha256();count=0
        with path.open('xb') as dst:
            for item in receipt_manifests:
                source=out/item['file']
                if hashlib.sha256(source.read_bytes()).hexdigest()!=item['sha256']:raise RuntimeError('Receipt file changed after validation.')
                with source.open('rb') as src:
                    for line in src:
                        if dst.write(line)!=len(line):raise RuntimeError('Short combined receipt write.')
                        digest.update(line);count+=1
            dst.flush();os.fsync(dst.fileno())
        if hashlib.sha256(path.read_bytes()).hexdigest()!=digest.hexdigest():raise RuntimeError('Combined receipts checksum mismatch.')
        return {'events':count,'sha256':digest.hexdigest(),'source':'actual journal ACK timestamps frozen from memory after each drained run'}
    class Listener(StreamingQueryListener):
        def onQueryStarted(self,event):pass
        def onQueryIdle(self,event):pass
        def onQueryTerminated(self,event):
            if event.exception:
                with lock:state['fatal']=str(event.exception)
        def onQueryProgress(self,event):
            record=json.loads(event.progress.json)
            if record.get('name')!='phuc_scoring':return
            with lock:
                rid=state['batch_runs'].get(record['batchId'])
                if rid:record['benchmark_run_id']=rid
                for src in record.get('sources',[]):
                    ends=src.get('endOffset')
                    if isinstance(ends,str):ends=json.loads(ends)
                    if ends and topic in ends:state['completed']={str(k):int(v) for k,v in ends[topic].items()}
            append(progress_file,record)
    listener=Listener();spark.streams.addListener(listener)
    def callback(batch,batch_id):
        start=time.perf_counter();batch=batch.cache()
        try:
            n=batch.count();materialized=time.perf_counter()
            if not n:return
            run_ids=[r['run_id'] for r in batch.select('run_id').distinct().collect()]
            if len(run_ids)!=1 or not run_ids[0]:raise RuntimeError(f'Mixed or missing run_id in batch {batch_id}: {run_ids}')
            rid=run_ids[0]
            with lock:state['batch_runs'][batch_id]=rid
            score_start=time.perf_counter()
            # Render historical site-local clock strings inside Spark. Python
            # datetime conversion otherwise follows the host TZ on collect().
            rows=bundle.score(batch.dropDuplicates(['event_id'])).withColumn('timestamp',F.date_format('timestamp','yyyy-MM-dd HH:mm:ss')).collect();scored=time.perf_counter()
            docs=[]
            for r in rows:
                d=r.asDict();d['_id']=d['event_id'];d['batch_id']=int(batch_id);docs.append(d)
            writes=0.0;receipt_seconds=0.0
            with receipt_file.open('a',encoding='utf-8') as receipts:
                for index in range(0,len(docs),500):
                    chunk=docs[index:index+500];t=time.perf_counter()
                    collection.bulk_write([UpdateOne({'_id':d['_id']},{'$set':d},upsert=True) for d in chunk],ordered=False)
                    ready=time.time_ns()//1_000_000;writes+=time.perf_counter()-t;t=time.perf_counter()
                    records=[{'run_id':rid,'batch_id':batch_id,'event_id':d['event_id'],'produced_at_ms':d['produced_at_ms'],'ready_at_ms':ready,'endpoint':'journaled_mongodb_sink'} for d in chunk]
                    receipts.write(''.join(json.dumps(x)+'\n' for x in records));receipts.flush()
                    with lock:
                        for x in records:
                            prev=state['deliveries'][rid].get(x['event_id'])
                            if prev is None or x['ready_at_ms']<prev['ready_at_ms']:state['deliveries'][rid][x['event_id']]=x
                    receipt_seconds+=time.perf_counter()-t
            append(batch_file,{'run_id':rid,'batch_id':batch_id,'input_rows':n,'scored_rows':len(docs),'invalid_or_duplicate_rows':n-len(docs),
                              'input_materialize_ms':(materialized-start)*1000,'score_collect_ms':(scored-score_start)*1000,
                              'mongo_write_ms':writes*1000,'receipt_write_ms':receipt_seconds*1000,'callback_ms':(time.perf_counter()-start)*1000})
            print(f'BATCH {batch_id} {rid} input={n} sink={len(docs)} callback={time.perf_counter()-start:.2f}s',flush=True)
        except Exception as error:
            with lock:state['fatal']=repr(error)
            raise
        finally:batch.unpersist()
    schema=T.StructType([T.StructField('building_id',T.StringType()),T.StructField('timestamp',T.StringType()),T.StructField('meter',T.IntegerType()),
                         T.StructField('meter_reading',T.DoubleType()),T.StructField('event_id',T.StringType()),T.StructField('run_id',T.StringType()),T.StructField('produced_at_ms',T.LongType())])
    source=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap).option('subscribe',topic).option('startingOffsets','earliest').load())
    parsed=source.select(F.from_json(F.col('value').cast('string'),schema).alias('event')).select('event.*')
    enriched=parsed.join(F.broadcast(meta),'building_id','left').join(F.broadcast(weather),['site_id','timestamp'],'left')
    query=(enriched.writeStream.queryName('phuc_scoring').foreachBatch(callback).option('checkpointLocation',str(out/'checkpoint')).trigger(processingTime=f'{a.trigger} seconds').start())
    processes={'benchmark_python':psutil.Process(metric_pid(os.getpid())),'kafka':psutil.Process(services['kafka']['pid']),'mongo':psutil.Process(services['mongo']['pid'])}
    gateway=spark.sparkContext._gateway
    if gateway.proc:processes['spark_jvm']=psutil.Process(metric_pid(gateway.proc.pid))
    for proc in processes.values():proc.cpu_percent()
    def latest():
        fut=admin.list_offsets({TopicPartition(topic,i):OffsetSpec.latest() for i in range(a.partitions)})
        return {str(tp.partition):int(f.result(timeout=10).offset) for tp,f in fut.items()}
    def offset_snapshot(marker=None):
        before=time.time_ns()//1_000_000
        with lock:completed=dict(state['completed']);phase=state['phase']
        ends=latest();now=time.time_ns()//1_000_000
        record={'sampled_at_ms':now,'offset_query_ms':now-before,'run_id':phase,'marker':marker,'latest_next_offsets':ends,'completed_next_offsets':completed,
                'backlog_messages':sum(ends[k]-completed.get(k,0) for k in ends),'processes':{}}
        if record['backlog_messages']<0:raise RuntimeError('Offset conventions or observation ordering invalid.')
        for name,proc in processes.items():
            try:
                cpu=proc.cpu_times()
                record['processes'][name]={'cpu_percent':proc.cpu_percent(),'cpu_seconds':cpu.user+cpu.system,'rss_bytes':proc.memory_info().rss}
            except psutil.NoSuchProcess:record['processes'][name]={'exited':True}
        append(sample_file,record);return record
    stopped=threading.Event()
    def monitor():
        while not stopped.is_set():
            try:offset_snapshot()
            except Exception as error:
                with lock:state['fatal']='monitor: '+repr(error)
                return
            stopped.wait(1)
    watcher=threading.Thread(target=monitor,daemon=True);watcher.start()
    producer=Producer({'bootstrap.servers':bootstrap,**configuration['producer_config']})
    def check():
        with lock:error=state['fatal']
        if error:raise RuntimeError(error)
        if not query.isActive:raise RuntimeError('Streaming query is not active.')
    def send(rid,rate,seconds):
        ack={'ok':0,'errors':[]};start=time.perf_counter();epoch=time.time_ns()//1_000_000;total=rate*seconds;payload_bytes=0
        def delivery(err,msg):
            if err:ack['errors'].append(str(err))
            else:ack['ok']+=1
        with lock:state['phase']=rid
        for i in range(total):
            due=start+i/rate
            while time.perf_counter()<due:
                producer.poll(0);time.sleep(min(.002,max(0,due-time.perf_counter())))
            if i%rate==0:check()
            event=dict(energy[i%len(energy)]);event.update(event_id=f'{rid}:{i:08d}',run_id=rid,produced_at_ms=time.time_ns()//1_000_000)
            payload=json.dumps(event,separators=(',',':')).encode();payload_bytes+=len(payload)
            while True:
                try:producer.produce(topic,key=event['building_id'],value=payload,on_delivery=delivery);break
                except BufferError:producer.poll(.01)
            producer.poll(0)
        while time.perf_counter()<start+seconds:producer.poll(0);time.sleep(.001)
        remaining=producer.flush(60)
        end=time.time_ns()//1_000_000;elapsed=time.perf_counter()-start
        if remaining or ack['errors'] or ack['ok']!=total:raise RuntimeError(f'Producer delivery failed: {ack}, queued={remaining}')
        return {'run_id':rid,'target_rate_mps':rate,'attempted_messages':total,'sent_messages':ack['ok'],'send_seconds':elapsed,'produced_start_ms':epoch,'producer_finished_ms':end,
                'payload_bytes_mean':payload_bytes/total,'producer_batch_bytes':65536,'partition_count':a.partitions,'trigger_seconds':a.trigger}
    def drain(rid,n):
        deadline=time.monotonic()+a.drain_timeout;heartbeat=time.monotonic()
        while time.monotonic()<deadline:
            check()
            with lock:delivered=state['deliveries'][rid];count=len(delivered)
            if count==n:
                snap=offset_snapshot('after_delivery')
                if snap['backlog_messages']==0:
                    with lock:last_ready=max(r['ready_at_ms'] for r in state['deliveries'][rid].values())
                    mongo_n=collection.count_documents({'run_id':rid})
                    if mongo_n!=n:raise RuntimeError(f'MongoDB count {mongo_n} differs from receipts {n}')
                    persist_receipts(rid,n)
                    return last_ready,snap,mongo_n
            if time.monotonic()-heartbeat>10:print(f'DRAIN {rid}: {count}/{n}',flush=True);heartbeat=time.monotonic()
            time.sleep(.25)
        raise RuntimeError(f'Drain timeout: {rid} delivered {count}/{n}')
    runs=[];warmups=[]
    def save_csv(path,rows):
        if not rows:return
        fields=list(dict.fromkeys(k for row in rows for k in row))
        with path.open('w',newline='',encoding='utf-8') as fp:w=csv.DictWriter(fp,fieldnames=fields);w.writeheader();w.writerows(rows)
    try:
        print('READY',exp,'rates',configuration['rates'],'repeats',a.repeats,'send',a.seconds,'warm',a.warmup,flush=True)
        for rate in configuration['rates']:
            for repeat in range(1,a.repeats+1):
                warm=f'{exp}_warm_r{rate}_n{repeat}';print('WARMUP',rate,'repeat',repeat,flush=True)
                row=send(warm,rate,a.warmup);last_ready,snapshot,n=drain(warm,row['sent_messages']);row['last_ready_ms']=last_ready;row['mongo_verified_count']=n;warmups.append(row);save_csv(out/'warmups.csv',warmups)
                if len(warmups)==1:
                    sample=list(collection.find({'run_id':warm}).limit(40))
                    errors=[abs(d['prediction']-reference[(d['building_id'],d['timestamp'])]) for d in sample]
                    if not sample or max(errors)>1e-8:raise RuntimeError(f'Online/offline prediction mismatch: {max(errors) if errors else None}')
                    append(out/'verification.jsonl',{'check':'online RF predictions agree with held-out offline reference','n':len(sample),'maximum_absolute_difference':max(errors),'status':'passed'})
                rid=f'{exp}_r{rate}_n{repeat}';print('MEASURE',rate,'repeat',repeat,flush=True)
                row=send(rid,rate,a.seconds);snapshot=offset_snapshot('producer_finished');row['backlog_end_messages']=snapshot['backlog_messages']
                last_ready,final_snapshot,n=drain(rid,row['sent_messages'])
                row.update(repeat=repeat,warmup_seconds=a.warmup,last_ready_ms=last_ready,observation_seconds=(last_ready-row['produced_start_ms'])/1000,
                           drain_seconds=max(0,(last_ready-row['producer_finished_ms'])/1000),backlog_after_drain_messages=final_snapshot['backlog_messages'],mongo_verified_count=n)
                runs.append(row);save_csv(out/'runs.csv',runs)
                print('DONE',rate,repeat,'sent',row['sent_messages'],'observed',round(row['observation_seconds'],3),'drain',row['drain_seconds'],'end_lag',row['backlog_end_messages'],flush=True)
        examples=list(collection.find({'run_id':{'$in':[r['run_id'] for r in runs]},'is_anomaly':True}).limit(20))
        (out/'mongo_alert_examples.json').write_text(json.dumps(examples,ensure_ascii=False,indent=2))
        samples=list(collection.find({'run_id':{'$in':[r['run_id'] for r in runs]}}).limit(100))
        (out/'mongo_scored_sample.json').write_text(json.dumps(samples,ensure_ascii=False,indent=2))
        combined=combine_receipts()
        expected=sum(r['sent_messages'] for r in runs+warmups)
        if combined['events']!=expected:raise RuntimeError('Combined receipt count mismatch.')
        (out/'completed.json').write_text(json.dumps({'experiment_id':exp,'status':'succeeded','measured_runs':len(runs),'measured_events':sum(r['sent_messages'] for r in runs),'warmup_events':sum(r['sent_messages'] for r in warmups),'mongo_verified':True,'canonical_receipts':combined},indent=2))
        print('COMPLETE measured runs',len(runs),'events',sum(r['sent_messages'] for r in runs),flush=True)
    finally:
        stopped.set();watcher.join(timeout=12)
        query.stop();spark.streams.removeListener(listener);spark.stop();mongo.close()

if __name__=='__main__':main()
