from doge import PrintingSink
from doge.doge import *
from examples.doge_example_common import create_example_data_online_generator


def format_function(timestamp: int, user: Subject, transition: Transition) -> str:
    return '[{}] User id: {}, balance: {}, loan_balance: {} made a transition {} from {} to {}'\
        .format(timestamp,
                user.user_id,
                user.balance,
                user.loan_balance,
                transition.trigger,
                transition.from_state,
                transition.to_state)


if __name__ == '__main__':
    sink = PrintingSink(format_function)

    datagen = create_example_data_online_generator(sink)

    datagen.start()