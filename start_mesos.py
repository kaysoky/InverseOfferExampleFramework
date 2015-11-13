import argparse
import atexit
import subprocess
import os
import tempfile

# Environment variable names.
MESOS_BIN_PATH   = 'MESOS_BIN_PATH'
TEST_NUM_AGENTS  = 'TEST_NUM_AGENTS'

# Mesos binary names.
MESOS_MASTER_BIN = 'mesos-master.sh'
MESOS_AGENT_BIN  = 'mesos-slave.sh'


def start_master(mesos_bin_path, work_dir):
    print 'Starting Mesos master at %s' % work_dir

    master = subprocess.Popen([
        os.path.join(mesos_bin_path, MESOS_MASTER_BIN),
        '--ip=127.0.0.1',
        '--work_dir=%s' % work_dir])

    assert master.returncode is None

    atexit.register(lambda: master.kill())


def start_agent(mesos_bin_path, work_dir, num=1):
    print 'Starting Mesos agent %d' % num

    agent = subprocess.Popen([
        os.path.join(mesos_bin_path, MESOS_AGENT_BIN),
        '--master=127.0.0.1:5050',
        '--work_dir=%s' % work_dir,
        '--ip=127.0.0.%d' % num,
        '--hostname=127.0.0.%d' % num])

    assert agent.returncode is None

    atexit.register(lambda: agent.kill())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Starts a mock local cluster.')
    parser.add_argument(
        '-n', type=int, required=True,
        help='Number of agents.')
    parser.add_argument(
        'mesos_bin_path', type=str,
        help='Path to the mesos binaries.')

    args = parser.parse_args()

    if args.n >= 254:
        print 'Cannot start more than 254 agents'
        exit(1)

    work_dir = tempfile.mkdtemp(prefix='inverse-offers')

    start_master(args.mesos_bin_path, work_dir)

    # You may need to run this beforehand:
    # rm -rf /tmp/mesos/meta/slaves/latest
    for i in range(args.n):
        start_agent(args.mesos_bin_path, work_dir, num=i + 1)

    print "Press enter to stop the mock cluster..."
    raw_input()
