from dataclasses import dataclass
from typing import Tuple
from unittest.mock import Mock

import pytest

from doge_datagen import DataOnlineGenerator, SubjectFactory


@dataclass
class SomeSubject:
    subject_id: int

    def __hash__(self):
        return self.subject_id


STATE_1 = 'state1'
STATE_2 = 'state2'
STATE_3 = 'state3'
TRANSITION_1 = 'transition1'
TRANSITION_2 = 'transition2'
SOME_SUBJECT_1 = SomeSubject(1)
SOME_SUBJECT_2 = SomeSubject(2)


class TestDoge:
    
    def test_should_transition_subjects(self):
        factory = self.__create_factory_mock([SOME_SUBJECT_1, SOME_SUBJECT_2])
        action_callback = Mock(return_value=True)
        event_sink = Mock()
        doge = DataOnlineGenerator([STATE_1, STATE_2, STATE_3],
                                   STATE_1,
                                   factory,
                                   2, 1000, 2)
        doge.add_transition(TRANSITION_1, STATE_1, STATE_2, 100, action_callback, [event_sink])
        doge.add_transition(TRANSITION_2, STATE_2, STATE_3, 100, action_callback, [event_sink])

        doge.start()

        assert action_callback.call_count == 4
        self.__assert_action_called(action_callback.call_args_list[0].args,
                                    SOME_SUBJECT_1, TRANSITION_1, STATE_1, STATE_2)
        self.__assert_action_called(action_callback.call_args_list[1].args,
                                    SOME_SUBJECT_2, TRANSITION_1, STATE_1, STATE_2)
        self.__assert_action_called(action_callback.call_args_list[2].args,
                                    SOME_SUBJECT_1, TRANSITION_2, STATE_2, STATE_3)
        self.__assert_action_called(action_callback.call_args_list[3].args,
                                    SOME_SUBJECT_2, TRANSITION_2, STATE_2, STATE_3)
        assert event_sink.collect.call_count == 4
        self.__assert_event_sink_collect_called(event_sink.collect.call_args_list[0].args,
                                                0, SOME_SUBJECT_1, TRANSITION_1, STATE_1, STATE_2)
        self.__assert_event_sink_collect_called(event_sink.collect.call_args_list[1].args,
                                                0, SOME_SUBJECT_2, TRANSITION_1, STATE_1, STATE_2)
        self.__assert_event_sink_collect_called(event_sink.collect.call_args_list[2].args,
                                                1000, SOME_SUBJECT_1, TRANSITION_2, STATE_2, STATE_3)
        self.__assert_event_sink_collect_called(event_sink.collect.call_args_list[3].args,
                                                1000, SOME_SUBJECT_2, TRANSITION_2, STATE_2, STATE_3)
        event_sink.close.assert_called()

    def test_should_fail_to_transition_once(self):
        factory = self.__create_factory_mock([SOME_SUBJECT_1])
        action_callback = Mock()
        action_callback.side_effect = [False, True]
        event_sink = Mock()
        doge = DataOnlineGenerator([STATE_1, STATE_2, STATE_3],
                                   STATE_1,
                                   factory,
                                   1, 1000, 2)
        doge.add_transition(TRANSITION_1, STATE_1, STATE_2, 100, action_callback, [event_sink])
        doge.add_transition(TRANSITION_2, STATE_2, STATE_3, 100, action_callback, [event_sink])

        doge.start()

        assert action_callback.call_count == 2
        self.__assert_action_called(action_callback.call_args_list[0].args,
                                    SOME_SUBJECT_1, TRANSITION_1, STATE_1, STATE_2)
        self.__assert_action_called(action_callback.call_args_list[1].args,
                                    SOME_SUBJECT_1, TRANSITION_1, STATE_1, STATE_2)
        assert event_sink.collect.call_count == 1
        self.__assert_event_sink_collect_called(event_sink.collect.call_args_list[0].args,
                                                1000, SOME_SUBJECT_1, TRANSITION_1, STATE_1, STATE_2)
        event_sink.close.assert_called()

    def test_should_raise_value_error_when_provided_incorrect_state_when_adding_transition(self):
        factory = self.__create_factory_mock([])
        doge = DataOnlineGenerator([STATE_1, STATE_2, STATE_3],
                                   STATE_1,
                                   factory,
                                   1, 1000, 2)

        with pytest.raises(ValueError):
            doge.add_transition(TRANSITION_1, 'unknown_state', STATE_2, 100)
        with pytest.raises(ValueError):
            doge.add_transition(TRANSITION_1, STATE_1, 'unknown_state', 100)

    def test_should_raise_value_error_when_provided_incorrect_initial_state(self):
        factory = self.__create_factory_mock([])
        with pytest.raises(ValueError):
            DataOnlineGenerator([STATE_1, STATE_2, STATE_3],
                                'unknown_state',
                                factory,
                                1, 1000, 2)

    @staticmethod
    def __create_factory_mock(subjects):
        factory = Mock(SubjectFactory)
        factory.create = Mock()
        factory.create.side_effect = subjects
        return factory

    @staticmethod
    def __assert_action_called(args: Tuple, subject: SomeSubject, trigger: str, from_state: str, to_state: str):
        assert args[0] == subject
        assert args[1].trigger == trigger
        assert args[1].from_state == from_state
        assert args[1].to_state == to_state

    @staticmethod
    def __assert_event_sink_collect_called(args: Tuple,
                                           timestamp: int,
                                           subject: SomeSubject,
                                           trigger: str,
                                           from_state: str,
                                           to_state: str):
        assert args[0] == timestamp
        assert args[1] == subject
        assert args[2].trigger == trigger
        assert args[2].from_state == from_state
        assert args[2].to_state == to_state
