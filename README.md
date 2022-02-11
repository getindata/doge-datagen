# Data Online Generator (doge_datagen)

[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/doge-datagen)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/doge-datagen.svg)](https://pypi.org/project/doge-datagen/)
[![Downloads](https://pepy.tech/badge/doge-datagen)](https://pepy.tech/badge/doge-datagen)

## Description
DataOnlineGenerator can be used to simulate user, system or other actor behaviour bases on probabilistic model. 
It is a state machine which is traversed by multiple subjects automatically based on probability defined for each 
possible transition. 

Each Subject instance can hold additional attributes that can be modified during transition with use of action_callback.
Action callback can also be used to make transition fail and to remain in current state.

In transition definition event_sinks can be passed which will be called on successful transition to log, generate event
or do other actions.

State machine works in ticks which length and number is defined in constructor. In each tick DataOnlineGenerate
evaluates each subject and makes a transition based on provided probabilities. Sum of probabilities of doing 
a transition from given state have to be less or equal to 100. If probabilities of all transitions are less than 100 
remaining value is treated as probability of staying in the same state in given tick.

## Installation

Package is available in PyPI https://pypi.org/project/doge-datagen/

```shell
pip install -U doge-datagen
```

## Usage

### DataOnlineGenerator
*Please refer to examples for full reference.*

Let's consider a user of a banking application that might take or consider taking a loan. Such user will have some
account and loan balance. Will be able to receive an income and spend his money. He will be also able to open his
banking application, open a loan screen and possible take a loan or exit application. 

We can model this behaviour as states and transitions:
![State machine](doc/states.png)

And define it as code:

```python
datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 10, 60000, 1000)
datagen.add_transition('income', 'offline', 'offline', 0.01,
                       action_callback=income_callback, event_sinks=[balance_sink])
datagen.add_transition('spending', 'offline', 'offline', 0.1,
                       action_callback=spending_callback, event_sinks=[trx_sink, balance_sink])
datagen.add_transition('login', 'offline', 'online', 0.1, event_sinks=[clickstream_sink])
datagen.add_transition('logout', 'online', 'offline', 70, event_sinks=[])
datagen.add_transition('open_loan_screen ', 'online', 'loan_screen', 30, event_sinks=[clickstream_sink])
datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_sinks=[clickstream_sink])
datagen.add_transition('take_loan', 'loan_screen', 'online', 10,
                       action_callback=take_loan_callback, event_sinks=[clickstream_sink, loan_sink, balance_sink])
```

Except defining state machine, we also provide factory that will be called to generate Subjects, in above example called
Users. We also provide tick length (1 min) and number of ticks (1000).

Probability of login into application 0.1 [%] can be interpreted as on average 1/1000 of all users in each minute will 
log in or that specific User will log in to an app on average once in 1000 minutes (a little more than once a day).

### Sink factories

#### Printing sink
Simple sink that can be used to print transition results on screen. It requires a format function that converts 
transition details into a string.

```python
def format_function(timestamp: int, user: Subject, transition: Transition) -> str:
    return '[{}] User id: {}, balance: {}, loan_balance: {} made a transition {} from {} to {}'\
        .format(timestamp,
                user.user_id,
                user.balance,
                user.loan_balance,
                transition.trigger,
                transition.from_state,
                transition.to_state)

sink = PrintingSink(format_function)
```

#### Kafka sink
Kafka sink allows emitting events into Kafka topics. By default, it uses String Serializers which can be overridden by
providing different serializers to `create` method. It requires `key_function` and `value_function` to be provided
which converts transition details into a format that is serializable by provided serializers.

```python
def key_function(subject: Subject, transition: Transition) -> str:
    return str(subject.user_id)

def value_function(timestamp: int, subject: Subject, transition: Transition) -> str:
    value = {
        'timestamp': timestamp,
        'user': {
            'user_id': subject.user_id,
            'balance': subject.balance,
            'loan_balance': subject.loan_balance
        },
        'event': transition.trigger
    }
    return json.dumps(value)

factory = KafkaSinkFactory(['localhost:9092'], 'doge-kafka-example')
sink = factory.create('test_topic', key_function, value_function)
```

#### Kafka Avro sink
`KafkaAvroSinkFactory` is a bit of convenience factory that wraps around regular `KafkaSinkFactory` that hides away creation
details of classes needed to push Avro events into Kafka. It requires functions that convert transition details into a
format suitable for `AvroSerializer` (typically it will be a structure of nested dicts, with sometimes tuples or type
hints in case of type unions in schema. 
[Fast avro documentation](https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take))
and avro schemas for key and value to be provided.

```python
def key_function(subject: Subject, transition: Transition) -> Dict[str, Any]:
    return {'key': str(subject.user_id)}

def value_function(timestamp: int, subject: Subject, transition: Transition) -> Dict[str, Any]:
    value = {
        'timestamp': timestamp,
        'user': {
            'userId': str(subject.user_id),
            'balance': str(subject.balance),
            'loanBalance': str(subject.loan_balance)
        },
        'event': transition.trigger
    }
    return value

def get_schema(schema_path):
    with open(schema_path) as f:
        return f.read()

key_schema = get_schema('./avro/Key.avsc')
event_schema = get_schema('./avro/Event.avsc')

factory = KafkaAvroSinkFactory(['localhost:9092'], 'http://localhost:8081', 'doge-kafka-example')
sink = factory.create('test_avro_topic', key_function, key_schema, value_function, event_schema)
```

#### DB sink
`DbSinkFactory` uses SQLAlchemy core. It requires DB Url in acceptable format and a function that is able to convert
transition details into a flat dict of values.

```python
def row_mapper_function(timestamp: int, subject: Subject, transition: Transition) -> Dict[str, Any]:
    row = {
        'timestamp': timestamp,
        'user_id': subject.user_id,
        'balance': subject.balance,
        'loan_balance': subject.loan_balance,
        'event': transition.trigger
    }
    return row

factory = DbSinkFactory('postgresql://postgres:postgres@localhost:5432/postgres')
sink = factory.create('events', row_mapper_function)
```