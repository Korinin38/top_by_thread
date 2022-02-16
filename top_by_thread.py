from collections import namedtuple, defaultdict
# input data
log_filename = "hz_all_nodes.log"
top_size = 10


def read_log(filename):
    Entry = namedtuple('LogEntry', 'datetime address type source thread data')
    with open(filename, "r") as file:
        lines = file.readlines()
    list_log = [l.strip().split("\t", 5) for l in lines]
    return [Entry(*line) for line in list_log]


def group_by_thread(log_entries):
    thread_group = defaultdict(list)
    for entry in log_entries:
        thread_group[entry.thread].append(entry)
    return thread_group


def print_top(threads_top, count):
    for line in threads_top[:count]:
        print(*line)


if __name__ == '__main__':
    log = read_log(log_filename)
    grouped_by_thread = group_by_thread(log)
    thread_count = [(pair[0], len(pair[1])) for pair in grouped_by_thread.items()]
    # sort in descending order
    thread_top = sorted(thread_count, key=lambda x: -x[1])
    print_top(thread_top, top_size)
