from typing import Callable

from doge_datagen import Subject, Transition, EventSink


class PrintingSink(EventSink):
    def __init__(self, format_function: Callable[[int, Subject, Transition], str]):
        self.format_function = format_function

    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        print(self.format_function(timestamp, subject, transition))
