import os
import signal
import subprocess
from tqdm import tqdm
import json
import time

def eval(skewed_request, broadcast, unif, percent_conflict, percent_write, protocol, num_reqs=100_000):
    # spawn master
    master_proc = subprocess.Popen("go run src/master/master.go", shell=True, preexec_fn=os.setsid)
    time.sleep(0.5)

    s1_proc = subprocess.Popen("go run src/server/server.go -port 7070 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)
    s2_proc = subprocess.Popen("go run src/server/server.go -port 7071 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)
    s3_proc = subprocess.Popen("go run src/server/server.go -port 7072 -exec -dreply " + protocol, shell=True, preexec_fn=os.setsid)

    # wait until subprocesses boot up
    time.sleep(4)

    command = "go run src/client/client.go -lat -through"
    if not skewed_request:
        command += " -e"
    if broadcast:
        command += " -f"
    if unif:
        command += " -unif"
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
    [skewed_request: bool, broadcast: bool, unif: bool, percent_conflict: list, percent_write: list, protocol: str, num_reqs: int]
    """
    [skewed_request, broadcast, unif, percent_conflicts, percent_writes, protocol, num_reqs] = params
    with open(f"{name}.json", "w+") as f:

        expts = form_tuples(percent_conflicts, percent_writes)
        print(f"{name} experiments: {expts}")

        skewed_expt_res = {}
        
        for expt in expts:
            # for each expt, run trial number of times
            (conf, wt) = expt
            skewed_expt_res[str(conf) + "," + str(wt)] = []
            for _ in tqdm(range(trials)):
                res = eval(skewed_request, broadcast, unif, conf, wt, protocol, num_reqs)
                skewed_expt_res[str(conf) + "," + str(wt)].append(res)
        
        json.dump(skewed_expt_res, f)

def conduct_lat_through_experiment(name, trials, params):
    """
    Conduct an experiment with the given name. Each setting
    will be run trial number of times. Result will be logged to "{name}.json"

    params is expected to be in the format
    [skewed_request: bool, broadcast: bool, unif: bool, percent_conflict: int, percent_write: int, protocol: str, num_reqs: list]
    """
    [skewed_request, broadcast, unif, conf, wt, protocol, num_reqs] = params
    with open(f"{name}.json", "w+") as f:

        print(f"{name} experiments: {num_reqs}")

        expt_res = {}
        
        for reqs in num_reqs:
            # for each expt, run trial number of times
            expt_res[str(reqs)] = []
            for _ in tqdm(range(trials)):
                res = eval(skewed_request, broadcast, unif, conf, wt, protocol, reqs)
                expt_res[str(reqs)].append(res)
        
        json.dump(expt_res, f)


#################################
# Experiments
#################################


# Evaluating effected of skewed client request vs egalitarian client requests
# over various conflict & write rates [-1, 0, 50, 100] x [0, 50, 100]
# at 100,000 requests
# 5 trial average

# conduct_experiment("skewed_workload_paxos", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "", 100_000])
# conduct_experiment("skewed_workload_mencius", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "-m", 100_000])
# conduct_experiment("skewed_workload_lambs", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "-l", 100_000])
# conduct_experiment("skewed_workload_epaxos", 5, [True, False, [-1, 0, 50, 100], [0, 50, 100], "-e", 100_000])

# conduct_experiment("egal_workload_paxos", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "", 100_000])
# conduct_experiment("egal_workload_mencius", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "-m", 100_000])
# conduct_experiment("egal_workload_lambs", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "-l", 100_000])
# conduct_experiment("egal_workload_epaxos", 5, [False, False, [-1, 0, 50, 100], [0, 50, 100], "-e", 100_000])
    


# Evaluating effected of skewed client request vs egalitarian client requests
# over 50 conflict vs 50 write, uniform vs bursty requests, 
# at 100,000 requests
# 5 trial average

# conduct_experiment("skewed_workload_paxos_bursty", 5, [True, False, False, [50], [50], "", 100_000])
# conduct_experiment("skewed_workload_paxos_unif", 5, [True, False, True, [50], [50], "", 100_000])
# conduct_experiment("skewed_workload_mencius_bursty", 5, [True, False, False, [50], [50], "-m", 100_000])
# conduct_experiment("skewed_workload_mencius_unif", 5, [True, False, True, [50], [50], "-m", 100_000])
# conduct_experiment("skewed_workload_epaxos_bursty", 5, [True, False, False, [50], [50], "-e", 100_000])
# conduct_experiment("skewed_workload_epaxos_unif", 5, [True, False, True, [50], [50], "-e", 100_000])

# conduct_experiment("egal_workload_paxos_bursty", 5, [False, False, False, [50], [50], "", 100_000])
# conduct_experiment("egal_workload_paxos_unif", 5, [False, False, True, [50], [50], "", 100_000])
# conduct_experiment("egal_workload_mencius_bursty", 5, [False, False, False, [50], [50], "-m", 100_000])
# conduct_experiment("egal_workload_mencius_unif", 5, [False, False, True, [50], [50], "-m", 100_000])
# conduct_experiment("egal_workload_epaxos_bursty", 5, [False, False, False, [50], [50], "-e", 100_000])
# conduct_experiment("egal_workload_epaxos_unif", 5, [False, False, True, [50], [50], "-e", 100_000])



# Evaluating P99 latency vs Throughput of skewed client request 
# over unif vs bursty client request
# 5 trial average

# conduct_lat_through_experiment("lt_lambs_bursty", 5, [True, False, False, 50, 50, "-l", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
# conduct_lat_through_experiment("lt_lambs_unif", 5, [True, False, True, 50, 50, "-l", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
# conduct_lat_through_experiment("lt_lambs_bursty_lite", 5, [True, False, False, 50, 50, "-l", [100, 500]])
# conduct_lat_through_experiment("lt_lambs_unif_lite", 5, [True, False, True, 50, 50, "-l", [100, 500]])
# conduct_lat_through_experiment("lt_mencius_bursty", 5, [True, False, False, 50, 50, "-m", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
# conduct_lat_through_experiment("lt_mencius_unif", 5, [True, False, True, 50, 50, "-m", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
conduct_lat_through_experiment("lt_mencius_bursty_lite", 5, [True, False, False, 50, 50, "-m", [100, 500]])
conduct_lat_through_experiment("lt_mencius_unif_lite", 5, [True, False, True, 50, 50, "-m", [100, 500]])
# conduct_lat_through_experiment("lt_epaxos_bursty", 5, [True, False, False, 50, 50, "-e", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
# conduct_lat_through_experiment("lt_epaxos_unif", 5, [True, False, True, 50, 50, "-e", [1000, 2500, 5000, 10_000, 50_000, 100_000]])
# conduct_lat_through_experiment("lt_epaxos_bursty_lite", 5, [True, False, False, 50, 50, "-e", [100, 500]])
# conduct_lat_through_experiment("lt_epaxos_unif_lite", 5, [True, False, True, 50, 50, "-e", [100, 500]])
