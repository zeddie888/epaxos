import os
import signal
import subprocess
from tqdm import tqdm
import json
import time

def eval(skewed_request, broadcast, percent_conflict, percent_write, protocol, num_reqs=100_000):
    # spawn master
    master_proc = subprocess.Popen("go run src/master/master.go", shell=True, preexec_fn=os.setsid)
    time.sleep(0.5)

    s1_proc = subprocess.Popen("go run src/server/server.go -port 7070 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)
    s2_proc = subprocess.Popen("go run src/server/server.go -port 7071 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)
    s3_proc = subprocess.Popen("go run src/server/server.go -port 7072 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)

    # wait until subprocesses boot up
    time.sleep(4)

    command = "go run src/client/client.go"
    if not skewed_request:
        command += " -e"
    if broadcast:
        command += " -f"
    command += f" -c {percent_conflict}"
    command += f" -w {percent_write}"
    command += f" -q {num_reqs}"

    print(f"running command: {command}")
    res = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True).stdout

    # terminate all spawned processes
    os.killpg(os.getpgid(master_proc.pid), signal.SIGTERM)
    os.killpg(os.getpgid(s1_proc.pid), signal.SIGTERM)
    os.killpg(os.getpgid(s2_proc.pid), signal.SIGTERM)
    os.killpg(os.getpgid(s3_proc.pid), signal.SIGTERM)

    time.sleep(4)

    return res



def form_tuples(conf_rs, wt_rs):
    res = []
    for conf in conf_rs:
        for wt in wt_rs:
            res.append((conf, wt))
    return res

def conduct_experiment(name, trials, params):
    """
    Conduct an experiment with the given name. Each setting
    will be run trial number of times. Result will be logged to "{name}.json"

    params is expected to be in the format
    [skewed_request: bool, broadcast: bool, percent_conflict: list, percent_write: list, protocol: str, num_reqs: int]
    """
    [skewed_request, broadcast, percent_conflicts, percent_writes, protocol, num_reqs] = params
    with open(f"{name}.json", "w+") as f:

        expts = form_tuples(percent_conflicts, percent_writes)
        print(f"{name} experiments: {expts}")

        skewed_expt_res = {}
        
        for expt in tqdm(expts):
            # for each expt, run trial number of times
            (conf, wt) = expt
            skewed_expt_res[str(conf) + "," + str(wt)] = []
            for _ in range(trials):
                res = eval(skewed_request, broadcast, conf, wt, protocol, num_reqs)
                skewed_expt_res[str(conf) + "," + str(wt)].append(res)
        
        json.dump(skewed_expt_res, f)


#################################
# Experiments
#################################


# Evaluating effected of skewed client request vs egalitarian client requests
# over various conflict & write rates [-1, 0, 50, 100] x [0, 50, 100]
# at 100,000 requests
# 5 trial average

# conduct_experiment("skewed_workload_paxos", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "", 100_000])
# conduct_experiment("skewed_workload_mencius", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "-m", 100_000])
# conduct_experiment("skewed_workload_epaxos", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "-e", 100_000])

# conduct_experiment("egal_workload_paxos", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "", 100_000])
# conduct_experiment("egal_workload_mencius", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "-m", 100_000])
conduct_experiment("egal_workload_epaxos", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "-e", 100_000])
    
