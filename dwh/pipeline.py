import re

from csv import DictReader
from collections import *
from itertools import chain
from xml.parsers.expat import ParserCreate
from unidecode import unidecode

from pathlib import Path
from typing import *
from typing.io import *
from typing.re import *

from .utils import dispatch_function, XMLStack, PersistentBuffer
from .configuration import Configurations

def normalize(field):
    if isinstance(field, str):
        field = unidecode(field)
    return field

@dispatch_function
def parse(file: IO, type: Text, **kwargs) -> Hashable:
    """Dispatch `file` to the appropriate parser (specified by `type`)"""

    return type.lower()


@parse.register('csv')
def _(file: IO, type: Text, **kwargs) -> Iterator[Dict]:
    """Parse a csv `file`"""

    yield from DictReader(file, **kwargs)


@parse.register('txt')
def _(file: IO, type: Text, pattern: Pattern, flags: int = 0) -> Iterator[Dict]:
    """Parse a text `file`"""

    yield from (item.groupdict() for item in re.finditer(pattern, file.read(), flags))


@parse.register('xml')
def _(file: IO, type: Text, buffer_size: int = 65536, buffer_text: bool = True) -> Iterator[Tuple]:
    """Parse an xml `file`"""

    parser = ParserCreate()
    parser.buffer_size = buffer_size
    parser.buffer_text = buffer_text

    stack = XMLStack()

    parser.StartElementHandler = stack.start
    parser.EndElementHandler = stack.end
    parser.CharacterDataHandler = stack.character

    for line in file:
        parser.Parse(line, False)
        yield from stack.items()
        stack.clear()


def project(values: Dict, mappings: Dict) -> Dict:
    """Performs a projection of `values` (row) to `mappings` (schema)"""

    return {(alias or field): normalize(values.get(field)) for (field, alias) in mappings.items()}


@dispatch_function
def dispatch(item: Union[Dict, Tuple], outputs: List[Dict]) -> Hashable:
    """Dispatch `item` to the appropriate dispatcher (based on its type)"""

    return type(item)


@dispatch.register(OrderedDict)
@dispatch.register(dict)
def _(item: Dict, outputs: List[Dict]) -> Iterator[Tuple[Text, Dict]]:
    """Dispatch `item` (row) to multiple `outputs` (tables)"""

    yield from ((output['name'], project(item, output['fields'])) for output in outputs)


@dispatch.register(tuple)
def _(item: Tuple, outputs: List[Dict]) -> Iterator[Tuple[Text, Dict]]:
    """Dispatch `item` (row) to multiple `outputs` (tables)"""

    tag, values = item
    yield from ((output['name'], project(values, output['fields'])) for output in outputs if tag == output['tag'])


def apply_pipeline(file: Path, config: Dict) -> Iterator[Tuple[Text, Dict]]:
    """Pass `file` through the pipeline (specified by `config`)"""

    if re.match(config.get('pattern'), file.name):
        with open(file, **config.get('source_args')) as f:
            yield from chain.from_iterable(dispatch(item, config.get('outputs')) for item in parse(f, **config.get('parser_args')))


def get_rows(file: Path, configs: List[Dict]) -> Iterator[Tuple[Text, Dict]]:
    """Pass `file` through all the pipelines (specified by `configs`)"""

    yield from chain.from_iterable((apply_pipeline(file, config) for config in configs))

def persist(pb: PersistentBuffer, config: Configurations, file):
    try:
        for table, row in get_rows(file, config.get()):
            pb.add(table, row)
    except Exception as e:
        raise e
    finally:
        file.unlink()
