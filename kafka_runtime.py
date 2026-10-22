"""Isolated Kafka runtime for the added ASHRAE experiment. No database service."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tarfile
import time
import urllib.request

KAFKA_VERSION = "3.9.1"
SPARK_VERSION = "3.5.6"
JARS = [("org.apache.spark", "spark-sql-kafka-0-10_2.12", SPARK_VERSION),
        ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", SPARK_VERSION),
        ("org.apache.kafka", "kafka-clients", "3.4.1"),
        ("org.apache.commons", "commons-pool2", "2.11.1")]


def fetch(url, destination):
    destination = Path(destination)
    if not destination.exists():
        temporary = destination.with_suffix(destination.suffix + ".part")
        with urllib.request.urlopen(url, timeout=60) as source, temporary.open("wb") as output:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                output.write(block)
        temporary.replace(destination)
    h = hashlib.sha256()
    with destination.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return {"file": destination.name, "bytes": destination.stat().st_size,
            "sha256": h.hexdigest(), "url": url}


class KafkaRuntime:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.process = None
        self.log = None
        self.bootstrap = "127.0.0.1:19092"

    def prepare(self):
        self.root.mkdir(parents=True, exist_ok=True)
        archive = f"kafka_2.13-{KAFKA_VERSION}.tgz"
        manifest = [fetch(f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{archive}", self.root / archive)]
        self.kafka = self.root / archive.removesuffix(".tgz")
        if not self.kafka.exists():
            with tarfile.open(self.root / archive, "r:gz") as source:
                source.extractall(self.root, filter="data")
        jars = self.root / "jars"
        jars.mkdir(exist_ok=True)
        for group, artifact, version in JARS:
            name = f"{artifact}-{version}.jar"
            url = f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{name}"
            manifest.append(fetch(url, jars / name))
        (self.root / "downloads.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return self

    def start(self):
        self.prepare()
        for port in (19092, 19093):
            with socket.socket() as sock:
                try:
                    sock.bind(("127.0.0.1", port))
                except OSError as error:
                    raise RuntimeError(f"Port {port} is busy; choose a free machine/session for the isolated pilot.") from error
        env = os.environ.copy()
        env["KAFKA_HEAP_OPTS"] = "-Xms256m -Xmx512m"
        state = self.root / "state"
        config = self.root / "server.properties"
        config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={state}
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.retention.hours=24
""", encoding="utf-8")
        if not (state / "meta.properties").exists():
            cluster = subprocess.check_output([str(self.kafka / "bin/kafka-storage.sh"), "random-uuid"], env=env, text=True).strip()
            subprocess.run([str(self.kafka / "bin/kafka-storage.sh"), "format", "-t", cluster, "-c", str(config)], env=env, check=True)
        self.log = (self.root / "kafka.log").open("a")
        self.process = subprocess.Popen([str(self.kafka / "bin/kafka-server-start.sh"), str(config)],
                                        env=env, stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": self.bootstrap, "log_level": 0})
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Kafka exited; see {self.root / 'kafka.log'}")
            try:
                admin.list_topics(timeout=2)
                return self
            except Exception:
                time.sleep(.5)
        raise RuntimeError("Kafka startup timed out.")

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log is not None:
            self.log.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download Kafka and matching Spark JARs only.")
    parser.add_argument("--runtime", type=Path, default=Path(__file__).parent / "runtime")
    args = parser.parse_args()
    KafkaRuntime(args.runtime).prepare()
    print("Kafka runtime downloaded.")
