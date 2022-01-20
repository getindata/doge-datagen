import time
from abc import ABC, abstractmethod
from datetime import datetime
from random import random
from typing import List, Dict, Callable, Iterable, Set, TypeVar, Generic

Subject = TypeVar('Subject')


class SubjectFactory(Generic[Subject]):
    """
    SubjectFactory creates instances of Subject class. In order to provide attributes to Subject class it is
    expected that SubjectFactory will be extended.
    """
    @abstractmethod
    def create(self) -> Subject:
        """
        Subject instance represents a single item that traverse defined state machine. It also can be used to hold data
        that can be modified and emitted during transitions. On its own it provides no methods nor attributes.
        It is expected that attributes will be provided by SubjectFactory during creation time.
        :return: a new Subject instance
        :rtype: Subject
        """
        pass


class EventSink(ABC):
    """
    Abstract base class for Sinks that will consume events emitted during state machine transitions.
    """
    @abstractmethod
    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        """
        Executed during state machine transition.
        :param timestamp: event timestamp
        :type timestamp: int
        :param subject: subject instance that makes a transition
        :type subject: Subject
        :param transition: transition instance on which subject travels
        :type transition: Transition
        """
        pass

    def close(self):
        """
        Executed at the end of DataOnlineGenerator.start() method execution. This method allows for graceful flush
        and close of any resource that was used as event sink
        """
        pass


class Transition(object):
    """
    Represents a possible transition in state machine.
    """
    def __init__(self,
                 trigger: str,
                 from_state: str,
                 to_state: str,
                 probability: float,
                 action_callback: Callable[[Subject, 'Transition'], bool] = None,
                 event_sinks: List[EventSink] = None):
        self.trigger = trigger
        self.from_state = from_state
        self.to_state = to_state
        self.probability = probability
        self.action_callback = action_callback
        self.event_sinks = event_sinks or ()


class DataOnlineGenerator(Generic[Subject]):
    """
    DataOnlineGenerator is a state machine which is traversed by multiple subjects automatically based on probability
    defined for each possible transition.

    State machine works in ticks which length and number is defined in constructor. In each tick DataOnlineGenerate
    evaluates each subject and makes a transition based on provided probabilities. Probabilities of doing a transition
    from given state have to be less or equal to 100. If probabilities of all transitions are less than 100 remaining
    value is treated as probability of staying in the same state in given tick.
    """
    STAY_TRIGGER = "__stay"
    transition_matrix: Dict[str, Dict[str, Transition]]
    probability_matrix: Dict[str, Dict[str, float]]
    subjects_states: Dict[Subject, str]
    sinks: Set[EventSink]

    def __init__(self,
                 states: List[str],
                 initial_state: str,
                 subject_factory: SubjectFactory,
                 subjects_num: int,
                 tick_ms: int,
                 ticks_num: int,
                 timestamp_start: int = 0):
        """
        :param states: list of all states in state machine
        :type states: List[str]
        :param initial_state: initial state for all subjects
        :type initial_state: str
        :param subject_factory: subject factory instance which will be used to create all subjects
        :type subject_factory: SubjectFactory
        :param subjects_num: number of subjects to create
        :type subjects_num: int
        :param tick_ms: length of a tick. It is assumed it is in milliseconds. It will be used to advance timestamp.
        :type tick_ms: int
        :param ticks_num: number of ticks to simulate
        :type ticks_num: int
        :param timestamp_start: optional starting timestamp
        :type timestamp_start: int
        """
        self.states = states
        self.__validate_state_defined(initial_state)
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

    def add_transition(self,
                       trigger: str,
                       from_state: str,
                       to_state: str,
                       probability: float,
                       action_callback: Callable[[Subject, 'Transition'], bool] = None,
                       event_sinks: List[EventSink] = None):
        """
        :param trigger: name of the trigger that triggers this transition. Has to be unique across all transitions
            from state :param from_state
        :type trigger: str
        :param from_state: name of the state transition points from
        :type from_state: str
        :param to_state: name of the state transition points to
        :type to_state: str
        :param probability: probability (in percentage, where 100 means certain probability) of Subject choosing this
            transition when being in state :param from_state
        :type probability: float
        :param action_callback: optional function that accepts Subject and Transition and returns a boolean value.
            It allows subject state manipulation and its return value is used as a condition for transition. If False
            is returned subject will stay in current state, transition will be not made and no event will be emitted.
        :type action_callback: Callable[[Subject, 'Transition'], bool]
        :param event_sinks: optional instance of EventSink class. During transition event_sink method collect will
            be called.
        :type event_sinks: EventSink
        """
        self.__validate_state_defined(from_state)
        self.__validate_state_defined(to_state)

        if event_sinks:
            self.sinks.update(event_sinks)

        transition = Transition(trigger, from_state, to_state, probability, action_callback, event_sinks)

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
            self.transition_matrix[state][self.STAY_TRIGGER] = \
                Transition(self.STAY_TRIGGER, state, state, stay_probability)
            self.probability_matrix[state][self.STAY_TRIGGER] = stay_probability

    def __validate_probability_sum(self, state: str):
        transitions = self.transition_matrix[state].values()
        probability_sum = self.__get_probability_sum(transitions)
        if probability_sum > 100:
            raise ValueError("Sum of probabilities for state", state, "have exceeded 100%!")

    def __validate_state_defined(self, state: str):
        if state not in self.states:
            raise ValueError('State {} is not present in machine states {}'.format(state, self.states))

    @staticmethod
    def __get_probability_sum(transitions: Iterable[Transition]) -> float:
        return sum(map(lambda transition: transition.probability, transitions))

    def __generate_subjects(self):
        for i in range(self.subjects_num):
            self.subjects_states[self.subject_factory.create()] = self.initial_state

    def __tick(self):
        for subject, state in self.subjects_states.items():
            trigger = self.__random_trigger(state)
            transition = self.transition_matrix[state][trigger]

            if not trigger == self.STAY_TRIGGER:
                should_transition = True
                if transition.action_callback:
                    should_transition = transition.action_callback(subject, transition)
                if should_transition:
                    self.subjects_states[subject] = transition.to_state
                    for sink in transition.event_sinks:
                        sink.collect(self.timestamp, subject, transition)

        self.timestamp += self.tick_ms

    # this is faster than using random.choice()
    def __random_trigger(self, state: str) -> str:
        rand = random() * 100
        cum = 0
        for key, probability in self.probability_matrix[state].items():
            cum += probability
            if rand < cum:
                return key

    def __close_sinks(self):
        for sink in self.sinks:
            sink.close()

    def start(self):
        start = time.time()
        print("Data generation start:", datetime.now())
        self.__generate_subjects()
        self.__add_stay_transitions()
        for i in range(self.ticks_num):
            self.__tick()
        self.__close_sinks()
        print("Data generation finished:", datetime.now())
        end = time.time()
        print("Execution took {} s".format(end - start))
