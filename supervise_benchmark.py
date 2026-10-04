"""Keep pilot services and the measurement in one execution session."""
from pathlib import Path
import argparse,json,os,signal,sys
import prepare_benchmark,run_benchmark
from process_metrics import metric_pid

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--smoke',action='store_true');a=p.parse_args()
    sys.argv=['prepare_benchmark.py','--runtime',str(a.runtime)]
    try:
        prepare_benchmark.main()
        sys.argv=['run_benchmark.py','--runtime',str(a.runtime),'--output',str(a.output)]
        if a.smoke:sys.argv+=['--rates','10','--repeats','1','--seconds','10','--warmup','5']
        run_benchmark.main()
    finally:
        pidfile=a.runtime/'services.json'
        if pidfile.exists():
            services=json.loads(pidfile.read_text())
            if services.get('owner_pid')==metric_pid(os.getpid()):
                for name in ['kafka','mongo']:
                    try:os.kill(services[name].get('namespace_pid',services[name]['pid']),signal.SIGTERM)
                    except ProcessLookupError:pass
                # Let both services finish their own shutdown/checkpoint before
                # the execution environment closes this process session.
                for process in prepare_benchmark.OWNED_PROCESSES.values():
                    try:process.wait(timeout=25)
                    except Exception as error:print('Service shutdown did not finish:',error,flush=True)

if __name__=='__main__':main()
