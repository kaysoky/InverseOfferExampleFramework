#!/usr/bin/env python

import json
import random
import requests
import time

if __name__ == '__main__':
    # Fetch all the agent info from the Master.
    slaves = requests.get('http://leader.mesos:5050/master/slaves')

    if slaves.status_code != 200:
        print slaves.text
        exit(1)

    # Grab all the hostnames.
    # We assume that the hostnames are the IPs as well.
    agents = [x['hostname'] for x in slaves.json()['slaves']]

    # Randomize the order of agents.
    random.shuffle(agents)

    # Convert to MachineIDs.
    machines = [{'hostname' : x, 'ip': x} for x in agents]

    # Convert to Maintenance Windows.
    epoch_nanos = int(time.time() * 1000) * 1000 * 1000
    duration = 3600 * 1000 * 1000 * 1000 # 1 hour.
    windows = []
    for i in range(len(machines)):
        window = {
            'machine_ids' : [machines[i]],
            'unavailability' : {
                'start' : {'nanoseconds' : epoch_nanos + (i * 3600) * 1000 * 1000 * 1000},
                'duration' : {'nanoseconds' : duration}
            }
        }
        windows.append(window)


    # Package up the windows into a Maintenance Schedule.
    # And post it!
    schedule = {'windows' : windows}

    result = requests.post('http://leader.mesos:5050/master/maintenance/schedule',
        data=json.dumps(schedule))

    print result, result.text

    # Print the list of machines in the order of maintenance.
    order = sorted(windows, lambda a, b: a['unavailability']['start']['nanoseconds'] - b['unavailability']['start']['nanoseconds'])
    order = [(x['machine_ids'][0]['ip'], x['unavailability']['start']['nanoseconds']) for x in order]
    print "Earliest"
    for ip, sec in order:
        print ip
    print "Latest"