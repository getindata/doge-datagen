from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Callable, Iterable, Set
from random import choices


class Subject(object):

    def __init__(self):
        return


class SubjectFactory(object):

    def create(self) -> Subject:
        return Subject()


class EventSink(ABC):
    @abstractmethod
    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        pass

    def close(self):
        pass


class Transition(object):

    def __init__(self, trigger: str, from_state: str, to_state: str, probability: float, action_callback: Callable[[Subject, 'Transition'], None] = None, event_sink: EventSink = None):
        self.trigger = trigger
        self.from_state = from_state
        self.to_state = to_state
        self.probability = probability
        self.action_callback = action_callback
        self.event_sink = event_sink


class DataOnlineGenerator(object):
    STAY_TRIGGER = "__stay"
    transition_matrix: Dict[str, Dict[str, Transition]]
    probability_matrix: Dict[str, Dict[str, float]]
    subject_states: Dict[Subject, str]
    sinks: Set[EventSink]

    def __init__(self, states: List[str], initial_state: str, subject_factory: SubjectFactory, subjects_num: int, tick_ms: int, ticks_num: int, timestamp_start: int =0):
        self.states = states
        self.initial_state = initial_state
        self.subject_factory = subject_factory
        self.subjects_num = subjects_num
        self.tick_ms = tick_ms
        self.ticks_num = ticks_num
        self.timestamp = timestamp_start
        self.transition_matrix = {}
        self.probability_matrix = {}
        self.subjects_states = {}
        self.sinks = set()

    def add_transition(self, trigger: str, from_state: str, to_state: str, probability: float, action_callback: Callable[[Subject, 'Transition'], None] = None, event_sink: EventSink = None):
        if from_state not in self.states or to_state not in self.states:
            raise ValueError('State is not present in machine states')

        if event_sink:
            self.sinks.add(event_sink)

        transition = Transition(trigger, from_state, to_state, probability, action_callback, event_sink)

        if from_state in self.transition_matrix:
            self.transition_matrix[from_state][trigger] = transition
            self.probability_matrix[from_state][trigger] = probability
        else:
            self.transition_matrix[from_state] = {trigger: transition}
            self.probability_matrix[from_state] = {trigger: probability}

        self.__validate_probability_sum(from_state)

    def __add_stay_transitions(self):
        for state, transitions in self.transition_matrix.items():
            stay_probability = 100 - self.__get_probability_sum(transitions.values())
            self.transition_matrix[state][self.STAY_TRIGGER] = Transition(self.STAY_TRIGGER, state, state, stay_probability)
            self.probability_matrix[state][self.STAY_TRIGGER] = stay_probability

    def __validate_probability_sum(self, state: str):
        transitions = self.transition_matrix[state].values()
        probability_sum = self.__get_probability_sum(transitions)
        if probability_sum > 100:
            raise ValueError("Sum of probabilities for state", state, "have exceeded 100%!")

    @staticmethod
    def __get_probability_sum(transitions: Iterable[Transition]) -> float:
        return sum(map(lambda transition: transition.probability, transitions))

    def __generate_subjects(self):
        for i in range(self.subjects_num):
            self.subjects_states[self.subject_factory.create()] = self.initial_state

    def __tick(self):
        for subject, state in self.subjects_states.items():
            trigger = choices(list(self.probability_matrix[state].keys()), list(self.probability_matrix[state].values()))
            transition = self.transition_matrix[state][trigger[0]]

            if not trigger[0] == self.STAY_TRIGGER:
                self.subjects_states[subject] = transition.to_state
                if transition.action_callback:
                    transition.action_callback(subject, transition)
                if transition.event_sink:
                    transition.event_sink.collect(self.timestamp, subject, transition)

        self.timestamp += self.tick_ms

    def __close_sinks(self):
        for sink in self.sinks:
            sink.close()

    def start(self):
        print("Data generation start:", datetime.now())
        self.__generate_subjects()
        self.__add_stay_transitions()
        for i in range(self.ticks_num):
            self.__tick()
        self.__close_sinks()
        print("Data generation finished:", datetime.now())


