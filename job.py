"""Checks data conformance against Petri Net models"""

import sys
import datetime as dt
import re
from pyspark.sql import SparkSession
import spark_tools.conformance_checking as cfc
from spark_tools.job_context import ConformanceCheckingContext
from spark_tools.line_parser import split_line
from spark_tools.line_parser import join_line

spark = SparkSession.builder.appName("conformance_checker").getOrCreate()

sc = spark.sparkContext



def main():
    """
    The main function executed by the job
    """
    CfcContext = ConformanceCheckingContext(sc)
    bucket_details = get_bucket_details()
    rawdata = get_raw_data(bucket_details)
    rawdata.cache()

    logs = extract_logs(rawdata)
    results = check_logs(logs, CfcContext)
    formatted_results = format_results(results, CfcContext)
    save_results(formatted_results, bucket_details)


def get_raw_data(bucket_details):
    """
    Retrieves the raw data from the messagereader bucket
    :param bucket_details: environment and date specifics
    :return: a spark RDD with a day's worth of raw data
    """
    if bucket_details[1] == 'pre01':
        folder = 'copiedFiles'
    else:
        folder = 'events'
    input_path = "s3a://%s-%s-dwh2-messagereader-s3/%s/%s" % \
                 (bucket_details[0], bucket_details[1], folder, bucket_details[2])
    input_rdd = sc.textFile(input_path)
    input_rdd = input_rdd.map(split_line)
    return input_rdd


def get_bucket_details():
    """
    Retrieve environment details and the path corresponding to the previous day's date
    :return: A string tuple with the datacenter, environment and datepath
    """
    datacenter = sys.argv[1]
    environment = sys.argv[2]
    datepath = re.sub('/0', '/', (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y/%m/%d'))
    return datacenter, environment, datepath


def extract_logs(input_rdd, first_events, last_events):
    """
    Processes the raw data and prepares it for validation against process models, by
    collating events.
    :param input_rdd: An rdd containing the rae event data
    :param filter_function: a function that extracts the particular events needed for this process model
    :param event_name_generator: a function that generates event names matching this in the process model
    from the raw data
    :param first_events: List of one or more events that can form the start of the process
    :param last_events: List of one or more events that can form the end of the process
    :return: An RDD with session logs in the format (ruid, [events in order])
    """
    sessions = group_sessions(input_rdd)
    sorted_sessions = sessions.map(sort_session)
    trimmed_sessions = trim_sessions(sorted_sessions, first_events, last_events)
    return trimmed_sessions


def extract_required_data(raw_data, event_name_generator):
    """
    formats a row to make it suitable for grouping into sessions
    :param raw_data: a row of raw data
    :param event_name_generator: function to generate event names from raw data
    :return: event record in the format [ruid, [timestamp, event name]]
    """
    RUID_INDEX = 8
    EVENT_DATE_TM_INDEX = 25
    required_data = raw_data.map(lambda x: [
        x[RUID_INDEX],
        [
            x[EVENT_DATE_TM_INDEX],
            event_name_generator(x)
        ]
    ])
    return required_data


def group_sessions(ungrouped_events):
    """
    Groups events for one ruid together into a single session
    :param un-grouped_events: RDD with un-grouped events
    :return: RDD with sessions in the format (ruid, [events in order])
    """
    grouped = ungrouped_events.aggregateByKey([[], []],
                                              group_with_zero_value,
                                              group_with_two_values)
    return grouped


def group_with_zero_value(event_list, zero_value):
    """
    initialises a timestamp/event representing a session for the first event found for a given ruid
    :param event_list: a list of events and a list with their respective timestamps
    in the form [[timestamps], [events]]
    :param zero_value: an empty set of event lists from which to initialise the output
    [[],[]]
    :return:
    """
    return [event_list[0]+[zero_value[0]], event_list[1]+[zero_value[1]]]


def group_with_two_values(event_list_a, event_list_b):
    """
    combines two timestamp/event list sets for the same ruid
    :param event_list_a: The first of two timestamp/event session logs to be combined
    in the form [[timestamps], [events]]
    :param event_list_b: the second log
    :return: a combined log
    """
    return [event_list_a[0]+event_list_b[0], event_list_a[1]+event_list_b[1]]


def find_error_sessions(log):
    """
    Identifies if there are any sessions containing error events in a session
    :param log: A session log in the form (ruid, [[timestamps][events]])
    :return: True if sessions contains errors, else False
    """
    errors = [True if re.search('error', x) or re.search('notification', x) else False for x in log[1][1]]
    return not any(errors)


def sort_session(log):
    """
    takes a session as a timestamp/event list set, and orders in based on the timestamps
    Returns only the event names.
    :param log: An session log for a ruid with event names and timestamps in the form
    [ruid, [[timestamps], [events]]]
    :return: An ordered session log without the timestamps in the form
    [ruid, [events in order]]
    """
    sort_zip = sorted(zip(log[1][0], log[1][1]))
    return log[0], [x for _, x in sort_zip]


def trim_sessions(sessions, first_events, last_events):
    """
    Takes an RDD with session logs and ensures they all start and end with the specified start and
    end events. Logs that do not contain at least one valid start event followed by an end event are
    filtered out.
    :param sessions: an RDD with session logs in the form [ruid, [events in order]]
    :param first_events: The names of the possible events a session should start with
    :param last_events: The names of the possible events a session should end with
    :return: An RDD with sessions starting and ending with the specified events
    """
    sessions_with_start = sessions.filter(lambda x: any([event in x[1] for event in first_events]))
    front_trimmed = sessions_with_start.map(lambda x: trim_start(x, first_events))
    complete_sessions = front_trimmed.filter(lambda x: any([event in x[1] for event in last_events]))
    fully_trimmed = complete_sessions.map(lambda x: trim_end(x, last_events))
    return fully_trimmed


def trim_start(log, first_events):
    """
    Trim the start of a session up to the first occurrence of the given start event.
    :param log: an session event log in the form [ruid, [events in order]]
    :param first_events: list of the initial events required in a session
    :return: the event log with any event before the first event removed
    """
    outlog = log[1]
    start = min([outlog.index(event) for event in first_events if event in outlog])
    outlog = outlog[start:len(outlog)]
    return log[0], outlog


def trim_end(log, last_events):
    """
    Trim the start of a session from the last occurrence of the given end event.
    :param log: an session event log in the form [ruid, [events in order]]
    :param last_events: list of the final events required in a session
    :return: the event log with any event after the last event removed
    """
    outlog = log[1]
    end = len(outlog) - min([outlog[::-1].index(event) for event in last_events if event in outlog])
    outlog = outlog[0:end]
    return log[0], outlog


def check_logs(logs, CfcContext):
    """
    validate the event logs for linear sessions
    :param logs: an RDD with session logs in the format [ruid, [events in order]]
    :param CfcContext: a Conformance Checking Context, containing a broadcast version of the ui model
    :return: An rdd with the conformance checking results in the form:
    [ruid, {missing_tokens=[], unconsumed_tokens=[]}]
    """
    results = logs.map(lambda x:
                       [x[0], cfc.check_log(CfcContext.broadcast_variables['linear_model'].value, x[1],
                                            expected_final_marking=['p2']), len(x[1])])
    return results


def format_results(results, area, CfcContext):
    """
    Formats the conformance checking results so that they are suitable for reporting
    :param results: An rdd with conformance checking results in the form
    [ruid, {missing_tokens=[], unconsumed_tokens=[]}]
    :param area: the model area that was validated
    :param CfcContext: Conformance checking context with the ui model file name
    :return: an rdd with one entry for each session in the form [ruid, area, model file name, missing tokens,
     unconsumed tokens, count of missing tokens, count of unconsumed tokens, session length]
    """
    formatted = results.map(lambda x: [
        x[0],
        area,
        (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00'),
        CfcContext.model_file_names[area],
        str(x[1]['missing_tokens']),
        str(x[1]['unconsumed_tokens']),
        str(len(x[1]['missing_tokens'])),
        str(len(x[1]['unconsumed_tokens'])),
        str(x[2])
    ])
    return formatted


def save_results(results, output_path):
    """
    Saves the conformance checking results to s3 and then copies them onto Redshift
    :param results: an RDD with conformance checking results
    :param area: the model area being checked
    :param bucket_details: the environment and date specific details of the output bucket
    """
    results_joined = results.map(join_line)
    results_joined.saveAsTextFile(output_path)

