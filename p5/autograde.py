import subprocess, time, os, argparse
import json, re # parsing JSON and regular expressions

from tester import init, test, tester_main, cleanup, error, warn, verbose, run_with_timeout, get_args
import nbutils

import argparse

ANSWERS = {} # global variable to store answers { key = question number, value = output of the answer cell }

@cleanup
def docker_cleanup():
    args = get_args()
    if args.skip_run:
        return
    
    try:
        subprocess.run(["docker compose kill; docker compose rm -f"], shell=True)
        subprocess.run(["docker", "rmi", "-f", "p5-base", "p5-nb", "p5-nn", "p5-dn", "p5-boss", "p5-worker"], check=True, shell=False)
        subprocess.run(["docker", "system", "prune", "-f"], check=True, shell=False)

        # remove any other running container in case 
        result = subprocess.run(["docker", "container", "ls", "-a"], capture_output = True, check=True, shell=False)
        stdout = result.stdout.decode('utf-8')
        if stdout.count("\n") > 1:
            warn(f"stopping other running containers to free up spaces:\n {stdout}")
            subprocess.run(["docker stop $(docker ps -a -q)"], check=True, shell=True)
            subprocess.run(["docker rm -f $(docker ps -a -q)"], check=True, shell=True)
    except:
        pass


def docker_startup():
    args = get_args()
    if args.skip_run:
        return
    
    print("================================== building docker images ==================================")
    subprocess.run(["docker", "build", ".", "-f", "p5-base.Dockerfile", "-t", "p5-base"])
    subprocess.run(["docker", "build", ".", "-f", "notebook.Dockerfile", "-t", "p5-nb"])
    subprocess.run(["docker", "build", ".", "-f", "namenode.Dockerfile", "-t", "p5-nn"])
    subprocess.run(["docker", "build", ".", "-f", "datanode.Dockerfile", "-t", "p5-dn"])
    subprocess.run(["docker", "build", ".", "-f", "boss.Dockerfile", "-t", "p5-boss"])
    subprocess.run(["docker", "build", ".", "-f", "worker.Dockerfile", "-t", "p5-worker"])

    print("================================== starting docker compose cluster ==================================")
    subprocess.run(["docker", "compose", "up", "-d"])

    check_hdfs_cmd = ["docker", "exec", "notebook", "hdfs", "dfsadmin", "-fs", "hdfs://nn:9000", "-report"]
    while True:
        try:
            output = subprocess.check_output(check_hdfs_cmd)
            m = re.search(r"Live datanodes \((\d+)\)", str(output, "utf-8"))
            if not m:
                verbose("report didn't describe live nodes")
            else:
                count = int(m.group(1))
                if count > 0:
                    print("HDFS ready!")
                    break
        except:
            verbose("couldn't get report from namenode")
        print("HDFS not ready ...")
        time.sleep(5)


def run_notebook():
    print("================================== running jupyter notebook ==================================")
    run_notebook_cmd = 'export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && python3 -m nbconvert --execute --to notebook nb/p5.ipynb --output tester-p5.ipynb'
    subprocess.run(["docker", "exec", "notebook", "sh", "-c", run_notebook_cmd])


def collect_cells(tester_nb='nb/tester-p5.ipynb'):
    global ANSWERS
    if os.path.exists(tester_nb):
        ANSWERS = nbutils.collect_answers(tester_nb)


@init
def init():
    args = get_args()
    if args.skip_run:
        collect_cells('nb/p5.ipynb')
    else:
        check_system_health()
        def wrapper():
            docker_cleanup()
            docker_startup()
            run_notebook()
        err = run_with_timeout(wrapper, 1200)
        if err:
            return err
        collect_cells()


@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    outputs = ANSWERS[1]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(106, output):
        return "Wrong answer"

@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    outputs = ANSWERS[2]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(106, output):
        return "Wrong answer"

@test(points=10)
def q3():
    if not 3 in ANSWERS:
        raise Exception("Answer to question 3 not found")
    outputs = ANSWERS[3]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(106, output):
        return "Wrong answer"


@test(points=10) # desugars to test(points=10)(q1) = wrapper(q1) -> TESTS["q1"] = _unit_test(q1, 10, None, "") 
def q4():
    if not 4 in ANSWERS:
        raise Exception("Answer to question 4 not found")
    outputs = ANSWERS[4]
    output = nbutils.parse_dict_bool_output(outputs)
    
    if not nbutils.compare_dict_bools(
        {'banks': False, 
        'loans': False, 
        'action_taken': True, 
        'counties': True, 
        'denial_reason': True, 
        'ethnicity': True, 
        'loan_purpose': True, 
        'loan_type': True, 
        'preapproval': True, 
        'property_type': True, 
        'race': True, 
        'sex': True, 
        'states': True, 
        'tracts': True
        }, output):
        return "Wrong answer"


@test(points=10)
def q5():
    if not 5 in ANSWERS:
        raise Exception("Answer to question 5 not found")
    outputs = ANSWERS[5]
    # print("test 5 outputs: ", outputs)
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(6, output):
        return "Wrong answer"

@test(points=10)
def q6():
    if not 6 in ANSWERS:
        raise Exception("Answer to question 6 not found")
    # to be manually graded


@test(points=10)
def q7():
    if not 7 in ANSWERS:
        raise Exception("Answer to question 7 not found")
    outputs = ANSWERS[7]
    # print("ANSWERS[7] = ", ANSWERS[7])
    output = nbutils.parse_dict_int_output(outputs)

    answer_with_duplicates = {
        'Sawyer': 38,
        'Door': 174,
        'Forest': 7,
        'Ozaukee': 389,
        'Bayfield': 33,
        'Waukesha': 1832,
        'Vilas': 68,
        'Dane': 729,
        'Oneida': 70,
        'Florence': 8
    }

    answer_with_uniques = {
        'Sawyer': 38,
        'Door': 87,
        'Forest': 7,
        'Ozaukee': 389,
        'Bayfield': 33,
        'Waukesha': 1832,
        'Vilas': 68,
        'Dane': 729,
        'Oneida': 70,
        'Florence': 8
    }
    
    if nbutils.compare_dict_ints(answer_with_duplicates, output):
        pass
    elif nbutils.compare_dict_ints(answer_with_uniques, output):
        pass
    else:
        return "Wrong answer"
        

@test(points=10)
def q8():
    if not 8 in ANSWERS:
        raise Exception("Answer to question 8 not found")
    # to be manually graded
    
    
@test(points=10)
def q9():
    if not 9 in ANSWERS:
        raise Exception("Answer to question 9 not found")
    outputs = ANSWERS[9]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(242868.0, output):
        return "Wrong answer"

@test(points=10)
def q10():
    if not 10 in ANSWERS:
        raise Exception("Answer to question 10 not found")
    outputs = ANSWERS[10]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.is_accurate(0.89, output):
        return "Wrong answer"
    
def check_system_health():
    try:
        # swap
        output = subprocess.check_output(['free', '-m']).decode('utf-8')
        lines = output.split('\n')
        swap_line = lines[2]
        swap_info = swap_line.split()
        total, used, free = int(swap_info[1]), int(swap_info[2]), int(swap_info[3])
        if total < 1000:
            error(f"Swap size ({total} MB) is much less than 1GB")
            raise Exception("Swap size is less than recommended")
        
        # disk
        output = subprocess.check_output(['df', '-h', '/']).decode('utf-8')
        usage_line = output.split('\n')[1]
        disk_usage = int(usage_line.split()[4][:-1])
        if disk_usage > 70:
            warn(f"Disk usage is {disk_usage}%")
            warn("If autograder fails, consider cleaning docker system storage")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--skip-run', action='store_true', help='Check answer without running the system')
    tester_main(parser, required_files=[
        "nb/p5.ipynb", 
        "boss.Dockerfile",
        "worker.Dockerfile",
        "namenode.Dockerfile",
        "datanode.Dockerfile",
        "notebook.Dockerfile",
        "p5-base.Dockerfile",
        "docker-compose.yml",
    ])
