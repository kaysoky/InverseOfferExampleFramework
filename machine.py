import argparse
import json
import requests

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Brings the given agent up or down.')
    parser.add_argument(
        'master', type=str,
        help='Host and port of the Mesos Master.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--up', action='store_true',
        help='Brings the machine UP.')
    group.add_argument(
        '--down', action='store_true',
        help='Brings the machine DOWN.')
    parser.add_argument(
        'agent', type=str,
        help='Hostname/IP of the agent to act upon.  We assume those are the same.')

    args = parser.parse_args()

    machines = [{'hostname' : args.agent, 'ip' : args.agent}]

    result = requests.post('http://%s/master/machine/%s' % (args.master, 'up' if args.up else 'down'),
        data=json.dumps(machines))

    print 'Response %d -- %s' % (result.status_code, result.text)
