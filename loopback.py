import argparse
import subprocess

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Opens or closes loopback interfaces.')
    parser.add_argument(
        '-n', type=int, required=True,
        help='Number of interfaces.')
    parser.add_argument(
        '--open', action='store_true',
        help='Opens up to N loopback interfaces.')

    args = parser.parse_args()

    if args.n >= 254:
        print 'Cannot open more than 255 total interfaces'
        exit(1)

    for i in range(args.n):
        subprocess.check_call(
            ['ifconfig', 'lo0', 'alias' if args.open else 'delete',
             '127.0.0.%d' % (i + 2)])