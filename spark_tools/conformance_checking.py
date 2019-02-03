"""Allows for conformance checking of an event log against a Petri Net."""

import xml.etree.ElementTree as ET
import copy


class Net(object):
    """
    A representation of a Petri net, consisting of a set of places and transitions.
    Provides methods to build the net and methods to allow replay of events on the model.
    """
    def __init__(self):
        self.places = {}
        self.transitions = {}
        self.transition_lookup = {}
        self.initial_marking = Marking()

    def add_place(self, new_id, new_name=''):
        """
        Adds a new place to the model. This is not actually needed for replay to function, but
        it allows the model to show a list of it's places, which is useful for debugging/analysis.
        :param new_id: A place id, consisting of 'p' and a number. Has to be unique in the model.
        :param new_name: A place name.
        """
        self.places[new_id] = {'name': new_name}

    def add_transition(self, new_id, name='', input_places=None, output_places=None):
        """
        Adds a new transition to the model. A lookup is kept, to allow quick retrieval of
        all transitions matching a given name.
        :param new_id: An id for the transition, consisting of 't' and a number. Must be unique in
        the model.
        :param name: A name for the transition. Must match the name of the events in the event log
        that should cause the transition to fire. Need not be unique.
        :param input_places: List of ids of input places leading into the transition.
        :param output_places: List of ids of output places the transition leads into.
        """
        if input_places is None:
            input_places = []
        if output_places is None:
            output_places = []
        self.transitions[new_id] = Transition(new_id, name, input_places, output_places)
        if self.transition_lookup.get(name) is None:
            self.transition_lookup[name] = [new_id]
        else:
            self.transition_lookup[name].append(new_id)

    def add_arc(self, source, target):
        """
        Adds an arc connecting a place to a transition or vice-versa.
        :param source: The id of the place or transition the arc originates from
        :param target: The id of the element the arc leads into. If the source is a
        transition this must be a place and vice-versa.
        """
        if source[0] == 'p':
            self.transitions[target].add_input_place(source)
        if source[0] == 't':
            self.transitions[source].add_output_place(target)

    def fire_transition_by_name(self, transition_name, marking):
        """
        Takes an event name and a marking and figures out the percentage of missing tokens for each transition with
        that name. It then finds all transitions with the smallest percentage of missing tokens. If there is only one
        it fires it. If there is more than one it raises a MultiPathError.
        :param transition_name: The name of an event
        :param marking: A marking that the event should be fired against.
        """
        eligible_transitions = self.transition_lookup.get(transition_name)
        if not eligible_transitions:
            raise UnknownEventError(transition_name)
        missing_ratios = []
        for transition_id in eligible_transitions:
            missing_ratio = self.test_transition(transition_id, marking)
            missing_ratios.append(missing_ratio)
        smallest_ratio = min(missing_ratios)
        eligible_transitions = [eligible_transitions[i] for i in range(0, len(missing_ratios))
                             if missing_ratios[i] == smallest_ratio]
        if len(eligible_transitions) == 1:
            self.fire_transition(eligible_transitions[0], marking)
        else:
            raise MultiPathError(possible_paths=eligible_transitions)

    def fire_transition(self, transition_id, marking):
        """
        Fires a transition.
        It does this by consuming a token from each input place where possible,
        logging an entry in the marking's missing token register if not, and by
        producing a token in each output place.
        :param transition_id: The transition to be fired.
        :param marking: The marking the transition is to be fired in.
        """
        transition = self.transitions[transition_id]
        for place in transition.input_places:
            if not marking.has_token(place):
                marking.missing_tokens.append(place)
                marking.produce_token(place)
        for place in transition.input_places:
            marking.consume_token(place)
        for place in transition.output_places:
            marking.produce_token(place)

    def test_transition(self, transition_id, marking):
        """
        Tests if a transition can be fired legally. Returns true if it can and raises a
        MissingTokenError if not.
        :param transition_id: The id of the transition to be tested.
        :param marking: The marking to be tested against.
        """
        transition = self.transitions[transition_id]
        missing_tokens = 0
        for place in transition.input_places:
            if place not in marking.tokens:
                missing_tokens += 1
        return float(missing_tokens)/len(transition.input_places)


class Transition(object):
    """
    Represents a transition in a Model. Stores the ids of places leading into and out if it.
    """
    def __init__(self, transition_id, name, input_places, output_places):
        self.transition_id = transition_id
        self.name = name
        self.input_places = input_places
        self.output_places = output_places

    def add_input_place(self, place):
        """
        Adds the id of a place leading into the transition.
        :param place: The id of the input place
        """
        self.input_places.append(place)

    def add_output_place(self, place):
        """
        Adds the id of a place the transition should lead into.
        :param place: The id of the output place
        """
        self.output_places.append(place)


class Marking(object):
    """
    represents the distribution of tokens across a network at a given point in time, as well as
    tracking how many missing tokens were encountered while re-playing the event log.
    """
    def __init__(self):
        self.tokens = []
        self.missing_tokens = []

    def __eq__(self, other):
        """
        Two markings are equivalent if they contain the same tokens and missing tokens,
        regardless of order
        """
        tokens_a = self.tokens
        tokens_a.sort()
        tokens_b = other.tokens
        tokens_b.sort()
        missing_tokens_a = self.tokens
        missing_tokens_a.sort()
        missing_tokens_b = other.tokens
        missing_tokens_b.sort()
        return tokens_a == tokens_b and missing_tokens_a == missing_tokens_b

    def has_token(self, place_id):
        """
        Checks if a token is present in a given place
        :param place_id: The id of the place to be checked
        """
        return place_id in self.tokens

    def consume_token(self, place_id):
        """
        Consumes a token from a given place.
        :param place_id: The id of the place to consume the token from
        """
        self.tokens.remove(place_id)

    def produce_token(self, place_id):
        """
        Produces a token in a given place.
        :param place_id: The id of the place to produce the token in.
        """
        self.tokens.append(place_id)


class MultiPathError(Exception):
    """
    Indicates that there are multiple legal paths that can be taken, providing a list of transitions
    that can be fired.
    """
    def __init__(self, possible_paths):
        super(MultiPathError, self).__init__()
        self.possible_paths = possible_paths
        self.message = 'found multiple fireable transitions: %s' % str(self.possible_paths)

    def __str__(self):
        return repr(self.message)


class UnknownEventError(Exception):
    """
    Indicates that there the event name to be fired is not in the model.
    """
    def __init__(self, eventName):
        super(UnknownEventError, self).__init__()
        self.eventName = eventName
        self.message = 'Event %s not found in Model' % str(self.eventName)

    def __str__(self):
        return repr(self.message)


class PathExplosionError(Exception):
    """
    Indicated that there are too many markings in the path buffer
    """
    def __init__(self, limit):
        super(PathExplosionError, self).__init__()
        self.limit = limit
        self.message = 'More than %s paths in buffer' % str(self.limit)

    def __str__(self):
        return repr(self.message)


def parse_pnml(filename):
    """
    Reads a .pnml file, and generates a corresponding Net() object.
    :param filename: The name of a .pnml file.
    :return: An object of Class Net
    """
    tree = ET.parse(filename)
    root = tree.getroot()
    places = root[0].findall('place')
    transitions = root[0].findall('transition')
    arcs = root[0].findall('arc')
    outnet = Net()
    for place in places:
        name = place.find('name').find('text').text
        place_id = place.get('id')
        outnet.add_place(place_id, name)
        if place.find('initialMarking') is not None:
            outnet.initial_marking.produce_token(place_id)
    for transition in transitions:
        name = transition.find('name').find('text').text
        outnet.add_transition(transition.get('id'), name)
    for arc in arcs:
        outnet.add_arc(source=arc.get('source'), target=arc.get('target'))
    return outnet


class LogChecker(object):
    """
    Keeps track of possible paths through a model based on an event log, and the degree to
    which each path diverges from conformance. Provides methods to replay events on the model.
    """
    def __init__(self, model, eventlog):
        self.model = model
        self.path_buffer = [copy.deepcopy(model.initial_marking)]
        self.eventlog = eventlog

    def check_events(self):
        """Passes over the events in the log and updates the path buffer"""
        for event in self.eventlog:
            self.update_markings(event)
            self.trim_path_buffer()

    def update_markings(self, event):
        """
        Takes an event from the log and fires it in each marking in the path buffer.
        If an event in the log would allow multiple possible transitions to be fired
        a new marking is added to the path buffer for each one.
        """
        for i in reversed(range(0, len(self.path_buffer))):
            try:
                self.model.fire_transition_by_name(event, self.path_buffer[i])
            except MultiPathError as error:
                for path in error.possible_paths:
                    alternate_marking = copy.deepcopy(self.path_buffer[i])
                    self.model.fire_transition(path, alternate_marking)
                    self.path_buffer.append(alternate_marking)
                del self.path_buffer[i]

    def trim_path_buffer(self):
        """
        Removes unnecessary markings from the path buffer. Only markings with the lowest
        number of deviations (missing tokens) are kept. Markings are also de-duplicated.
        If the number of marking becomes too great an error is thrown.
        """
        path_limit = 1000
        if len(self.path_buffer) > 1:
            self.keep_least_divergent_markings()
            self.delete_duplicate_markings()
            if len(self.path_buffer) > path_limit:
                raise PathExplosionError(path_limit)

    def keep_least_divergent_markings(self):
        """
        Works out the smallest number of missing tokens among all markings in the path
        buffer and deletes any markings that have more than that.
        """
        diversion_degree = [len(marking.missing_tokens) for marking in self.path_buffer]
        smallest_diversion = min(diversion_degree)
        for i in reversed(range(0, len(self.path_buffer))):
            if diversion_degree[i] > smallest_diversion:
                del self.path_buffer[i]

    def delete_duplicate_markings(self):
        """
        Deletes any duplicate markings from the path buffer
        """
        dupes = [n for n, x in enumerate(self.path_buffer) if x in self.path_buffer[:n]]
        if dupes:
            for dupe in reversed(dupes):
                del self.path_buffer[dupe]

    def resolve_expected_final_marking(self, expected_final_marking):
        """
        Takes the first marking left in the path buffer as the final marking.
        If an expected final marking is provided, it resolves it by consuming the respective
        tokens so that they do not count against the conformance in the final results.
        (Or do if they are missing.)
        :param expected_final_marking: List of places that are expected to contain tokens at the
        end of a process
        :return: The final marking with remaining and missing tokens, after the expected final
        marking has been resolved
        """
        final_marking = self.path_buffer[0]
        if expected_final_marking:
            for token in expected_final_marking:
                final_marking.consume_token(token)
        return final_marking


def format_results(final_marking):
    """
    Generates a dict with a list of all missing and unconsumed tokens.
    :param final_marking: The final marking created from replaying an event log on a model.
    :return: A dict containing lists of missing and unconsumed tokens.
    """
    results = {
        'missing_tokens': final_marking.missing_tokens,
        'unconsumed_tokens': final_marking.tokens
    }
    return results


def check_log(model, eventlog, expected_final_marking=None):
    """
    Replays an event log against a Petri net and returns the conformance of the most fitting path
    :param model: A petri net process model
    :param eventlog: A list of events in sequential order to be checked against the model
    :param expected_final_marking: A list of places that a model is expected to end up in
    :return: A set of results with lists of any missing or unconsumed tokens.
    """
    checker = LogChecker(model, eventlog)
    try:
        checker.check_events()
    except UnknownEventError as error:
        return {'missing_tokens': 'unknown Event: %s' % error.eventName, 'unconsumed_tokens': []}
    except PathExplosionError as error:
        return {'missing_tokens': 'Path Explosion: over %s paths in buffer' % error.limit,
                'unconsumed_tokens': []}
    final_marking = checker.resolve_expected_final_marking(expected_final_marking)
    results = format_results(final_marking)
    return results
