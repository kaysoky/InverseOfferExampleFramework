import argparse
import json
import random
import requests
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Constructs a random sliding maintenance schedule.')
    parser.add_argument(
        'master', type=str,
        help='Host and port of the Mesos Master.')
    parser.add_argument(
        '-d', '--dry-run', action='store_true',
        help='Just prints the schedule.')

    args = parser.parse_args()

    # Fetch all the agent info from the Master.
    state = requests.get('http://%s/master/state.json' % args.master)

    if state.status_code != 200:
        print state.text
        exit(1)

    # Grab all the hostnames.
    # We assume that the hostnames are the IPs as well.
    agents = [x['hostname'] for x in state.json()['slaves']]

    # Randomize the order of agents.
    random.shuffle(agents)

    # Convert to MachineIDs.
    machines = [{'hostname' : x, 'ip': x} for x in agents]

    # Convert to Maintenance Windows.
    epoch_nanos = int(time.time() * 1000) * 1000 * 1000
    duration = 60 * 1000 * 1000 * 1000
    windows = [{'machine_ids' : [x],
                'unavailability' : {
                    'start' : {'nanoseconds' : epoch_nanos + (random.randint(0, len(machines) * 60) * 1000 * 1000 * 1000)},
                    'duration' : {'nanoseconds' : duration}
                }} for x in machines]

    # Package up the windows into a Maintenance Schedule.
    # And post it!
    schedule = {'windows' : windows}
    if args.dry_run:
        print json.dumps(schedule, indent=2)
    else:
        result = requests.post('http://%s/master/maintenance/schedule' % args.master,
            data=json.dumps(schedule))

        print result, result.text

    # Print the list of machines in the order of maintenance.
    order = sorted(windows, lambda a, b: a['unavailability']['start']['nanoseconds'] - b['unavailability']['start']['nanoseconds'])
    order = [(x['machine_ids'][0]['ip'], x['unavailability']['start']['nanoseconds']) for x in order]
    print "Earliest"
    for ip, sec in order:
        print ip
    print "Latest"
