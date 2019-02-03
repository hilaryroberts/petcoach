"""Splits and joins pipe delimited lines of data"""

def split_line(line):
    """
    splits a pipe-delimited line of data

    :param line: string of pipe delimited data
    :return: list of strings
    """
    return line.split('|')


def join_line(line):
    """
    joins a list of strings into a single pipe-delimited string

    :param line: list of strings
    :return: single pipe-delimited string
    """
    return '|'.join(line)
