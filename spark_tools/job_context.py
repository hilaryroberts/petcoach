"""JobContext classes to handle distributed data like broadcast variables and counters"""

import re
import os
import zipfile
from collections import OrderedDict
import spark_tools.conformance_checking as cfc


class JobContext(object):
    """Handles broadcast variables and counters for the spark job."""

    def __init__(self, spark_context):
        self.counters = OrderedDict()
        self.broadcast_variables = OrderedDict()
        self._init_accumulators(spark_context)
        self._init_shared_data(spark_context)

    def _init_accumulators(self, spark_context):
        pass

    def _init_shared_data(self, spark_context):
        pass

    def initalize_counter(self, spark_context, name):
        """Add a new counter"""
        self.counters[name] = spark_context.accumulator(0)

    def inc_counter(self, name, value=1):
        """Increment a counter"""
        if name not in self.counters:
            raise ValueError("%s counter was not initialized. (%s)" % (name, self.counters.keys()))
        self.counters[name] += value

    def reset_counter(self, name, value=0):
        """reset a counter"""
        if name not in self.counters:
            raise ValueError("%s counter was not initialized. (%s)" % (name, self.counters.keys()))
        self.counters[name]._value = value

    def initialize_broadcast_variable(self, spark_context, name, value):
        """Add a new broadcast variable"""
        self.broadcast_variables[name] = spark_context.broadcast(value)


class ConformanceCheckingContext(JobContext):
    """
    Specific JobContext implementation for conformance checking.

    On initialisation it deternines the name of each model file based on prefix, loads it
    and broadcasts it.

    :param spark_context: the spark context providing data handling functions
    :return: a Job Context with a distributed set of model files and their names.
    """

    def __init__(self, spark_context):
        extract_models()
        super(ConformanceCheckingContext, self).__init__(spark_context)

    def _init_shared_data(self, spark_context):
        """Retrieves the ruid mapping and broadcast it to the worker nodes."""
        model = cfc.parse_pnml('models/model1')
        self.initialize_broadcast_variable(spark_context, name='model1', value=model)


def extract_models():
    """Unizips the folder with model files"""
    zip_ref = zipfile.ZipFile('models.zip', 'r')
    zip_ref.extractall()
    zip_ref.close()
