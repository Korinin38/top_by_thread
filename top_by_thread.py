from collections import namedtuple, defaultdict
# input data
log_filename = "hz_all_nodes.log"
top_size = 10
field_list = ["datetime", "address", "type", "source", "thread", "data"]


def read_log(filename, fields):
    '''
    Reads log from given file into manageable format
    :param filename: path to log file
    :param fields: list of fields of log entry
    :return: list of log entries
    '''
    Entry = namedtuple("LogEntry", " ".join(fields))
    with open(filename, "r") as file:
        lines = file.readlines()
    list_log = [l.strip().split("\t", len(fields) - 1) for l in lines]
    return [Entry(*line) for line in list_log]


def group_by(log_entries, field):
    '''
    Transforms Sequence of log entries into dict with keys of 'field' values
    and value of list of entries that correspond to that 'field' value
    :param log_entries: Sequence of log entries
    :param field: attribute name of log entries to group by
    :return: dict {field_data: [list_of_entries_with_given_data_in_field]}
    '''
    thread_group = defaultdict(list)
    for entry in log_entries:
        thread_group[getattr(entry, field)].append(entry)
    return thread_group


def print_top(threads_top, count):
    '''
    Prints top-'count' in readable format
    :param threads_top: sorted list of pairs (key, sorted_value)
    :param count: how many of entries to include in top
    :return: None
    '''
    for line in threads_top[:count]:
        print(*line)


if __name__ == '__main__':
    log = read_log(log_filename, field_list)
    grouped_by_thread = group_by(log, "thread")
    thread_count = [(item[0], len(item[1])) for item in grouped_by_thread.items()]
    # lambda: sort in descending order
    thread_top = sorted(thread_count, key=lambda x: -x[1])
    print_top(thread_top, top_size)
