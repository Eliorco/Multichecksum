import json
import multiprocessing

import multichecksum


def main():
    c = multichecksum.Checker()
    c.concurrency = 4
    multiprocess_check_dir = c.check_dir_multi(
        "./tst")
    singleprocess_check_dir = c.check_dir("./tst")
    print(json.dumps(dict(multiprocess_check_dir), indent=2))
    print(json.dumps(dict(singleprocess_check_dir), indent=2))


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
