"""Start an isolated, loopback-only Kafka 3.9.1 and MongoDB 7.0.18 pilot."""
from pathlib import Path
import argparse, concurrent.futures, hashlib, json, os, socket, subprocess, tarfile, time, urllib.request

ROOT = Path(__file__).resolve().parent
OWNED_PROCESSES = {}
from process_metrics import metric_pid
ARCHIVES = [
    ('kafka_2.13-3.9.1.tgz','https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz'),
    ('mongodb-linux-x86_64-ubuntu2204-7.0.18.tgz','https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-7.0.18.tgz')]
JARS = [('org.apache.spark','spark-sql-kafka-0-10_2.12','3.5.6'),
        ('org.apache.spark','spark-token-provider-kafka-0-10_2.12','3.5.6'),
        ('org.apache.kafka','kafka-clients','3.4.1'),('org.apache.commons','commons-pool2','2.11.1')]

def fetch(url,path):
    if not path.exists():
        with urllib.request.urlopen(url,timeout=40) as src,path.open('wb') as out:
            while block:=src.read(1024*1024):out.write(block)
    return {'file':path.name,'url':url,'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def free_port(port):
    with socket.socket() as sock:
        try:sock.bind(('127.0.0.1',port))
        except OSError:raise RuntimeError(f'Port {port} is busy. Stop only the pilot services that you own.')

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--runtime',type=Path,default=ROOT/'runtime');p.add_argument('--stop',action='store_true');a=p.parse_args()
    runtime=a.runtime.resolve();runtime.mkdir(parents=True,exist_ok=True)
    pid_file=runtime/'services.json'
    if a.stop:
        import signal
        if not pid_file.exists():return
        services=json.loads(pid_file.read_text())
        import psutil
        for name in ['kafka','mongo']:
            item=services[name]
            try:
                process=psutil.Process(item['pid'])
                if abs(process.create_time()-item['create_time'])>.1:raise RuntimeError('PID was reused; refusing to stop another process.')
                process.send_signal(signal.SIGTERM)
            except psutil.NoSuchProcess:pass
        print('Stopped owned pilot services.');return
    if pid_file.exists():
        import psutil
        saved=json.loads(pid_file.read_text())
        def still_owned(item):
            try:return abs(psutil.Process(item['pid']).create_time()-item['create_time'])<.1
            except psutil.NoSuchProcess:return False
        if all(still_owned(saved[x]) for x in ['kafka','mongo']):
            print('Pilot services are already running.');return
        pid_file.unlink()
    for port in [19092,19093,27019]:free_port(port)
    manifests=[]
    def archive(item):
        name,url=item;path=runtime/name;info=fetch(url,path)
        folder=runtime/name.removesuffix('.tgz')
        if not folder.exists():
            with tarfile.open(path,'r:gz') as tf:tf.extractall(runtime,filter='data')
        return info
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:manifests.extend(pool.map(archive,ARCHIVES))
    jars=runtime/'jars';jars.mkdir(exist_ok=True)
    for group,artifact,version in JARS:
        name=f'{artifact}-{version}.jar';url=f'https://repo.maven.apache.org/maven2/{group.replace(".","/")}/{artifact}/{version}/{name}'
        manifests.append(fetch(url,jars/name))
    (runtime/'downloads_manifest.json').write_text(json.dumps(manifests,indent=2))
    env=os.environ.copy()
    java17=Path('/usr/lib/jvm/java-17-openjdk-amd64')
    if java17.exists():env['JAVA_HOME']=str(java17);env['PATH']=str(java17/'bin')+os.pathsep+env['PATH']
    env['KAFKA_HEAP_OPTS']='-Xms256m -Xmx512m'
    kafka=runtime/'kafka_2.13-3.9.1';state=runtime/'state';state.mkdir(exist_ok=True)
    kafka_state=state/'kafka';mongo_state=state/'mongodb';mongo_state.mkdir(exist_ok=True)
    config=runtime/'server.properties'
    config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={kafka_state}
num.partitions=3
num.network.threads=3
num.io.threads=4
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.retention.hours=24
''')
    if not (kafka_state/'meta.properties').exists():
        cluster=subprocess.check_output([str(kafka/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip()
        subprocess.run([str(kafka/'bin/kafka-storage.sh'),'format','-t',cluster,'-c',str(config)],env=env,check=True)
    kafka_log=(runtime/'kafka_server.log').open('a')
    kp=subprocess.Popen([str(kafka/'bin/kafka-server-start.sh'),str(config)],env=env,stdout=kafka_log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
    OWNED_PROCESSES['kafka']=kp
    mongo_log=(runtime/'mongo_server.log').open('a')
    mp=subprocess.Popen([str(runtime/'mongodb-linux-x86_64-ubuntu2204-7.0.18/bin/mongod'),'--dbpath',str(mongo_state),'--bind_ip','127.0.0.1','--port','27019','--nounixsocket','--wiredTigerCacheSizeGB','0.25','--setParameter','diagnosticDataCollectionEnabled=false','--quiet'],stdout=mongo_log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
    OWNED_PROCESSES['mongo']=mp
    import psutil
    services={'kafka':{'pid':metric_pid(kp.pid),'namespace_pid':kp.pid,'create_time':psutil.Process(metric_pid(kp.pid)).create_time()},'mongo':{'pid':metric_pid(mp.pid),'namespace_pid':mp.pid,'create_time':psutil.Process(metric_pid(mp.pid)).create_time()},'owner_pid':metric_pid(os.getpid()),'runtime':str(runtime),'bootstrap_servers':'127.0.0.1:19092','mongo_uri':'mongodb://127.0.0.1:27019','java_home':env.get('JAVA_HOME')}
    pid_file.write_text(json.dumps(services,indent=2))
    from confluent_kafka.admin import AdminClient
    from pymongo import MongoClient
    admin=AdminClient({'bootstrap.servers':'127.0.0.1:19092'});mongo=MongoClient('mongodb://127.0.0.1:27019',serverSelectionTimeoutMS=1000)
    end=time.monotonic()+60
    while time.monotonic()<end:
        if kp.poll() is not None or mp.poll() is not None:raise RuntimeError('Pilot service exited; inspect its log in --runtime.')
        try:admin.list_topics(timeout=2);mongo.admin.command('ping');break
        except Exception:time.sleep(.5)
    else:raise RuntimeError('Pilot services did not become ready in 60 seconds.')
    print('Ready: Kafka 3.9.1 at 127.0.0.1:19092; MongoDB',mongo.server_info()['version'],'at 127.0.0.1:27019',flush=True)
    mongo.close()

if __name__=='__main__':main()
