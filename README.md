# Template Script Data Processor

![Generic badge](https://img.shields.io/badge/Author-José%20Antonio%20Perdiguero%20López-blue.svg)
![Generic badge](https://img.shields.io/badge/Status-Development-yellow.svg)

## Introduction

TODO.

## System Requirements

 * [Python]
 * [Poetry]

## Quick Start

After installing all the system requirements it's necessary to install the project requirements running the following 
command under project directory (low_level_challenge):

```commandline
poetry install
```

Since this moment all interactions with the Python interpreter will be under Poetry's umbrella, to make sure we're using 
the right interpreter (the virtual one created in previous step).

The project contains a self-description help:

```commandline
poetry run python . --help
```

### Run the script

And the script itself can be run using:

```commandline
poetry run python . run <output_path> -d <input_dataset_1> -d <input_dataset_2>
```

### Tests

The tests can be executed by:

```commandline
poetry run pytest
```

[Python]: https://www.python.org/downloads/.
[Poetry]: https://poetry.eustace.io/docs/#installation.
[Clinner]: https://github.com/perdy/clinner.
