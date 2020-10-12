import sys
import getopt
import json
import pyrebase


firebase_config_file = 'firebase_config.json'


def firebase_cmd(cmd, params):
    with open(firebase_config_file) as f:
        firebase_config = json.load(f)

    firebase_connect = firebase_config['firebase_connect']
    firebase = pyrebase.initialize_app(firebase_connect)
    storage = firebase.storage()

    if cmd == 'upload':
        storage.child(params['remote']).put(params['local'])
    elif cmd == 'download':
        storage.child(params['remote']).download(params['local'])


def main():
    params = {}

    try:
        cmd, argv = sys.argv[1], sys.argv[2:]

        # parse command line options
        opts, args = getopt.getopt(argv, 'k:')
        options = dict(opts)
        if '-k' in options.keys():
            params['credential'] = Options['-k']

        if cmd in ('upload', 'download'):
            remote, local = argv
            params['remote'] = remote
            params['local'] = local
        else:
            print("unknown command")
            sys.exit(1)
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)
    except (IndexError, ValueError):
        print(f"syntax:\n\t{sys.argv[0]} command param1 param2 ...")
        sys.exit(2)

    firebase_cmd(cmd, params)


if __name__ == '__main__':
    main()
