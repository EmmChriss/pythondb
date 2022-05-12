# pythondb
Custom SQL-like database with a minimally used MongoDB backend

![screen](misc/screen.png)

## Installation

### Dev

I use [virtualenv](https://virtualenv.pypa.io/en/stable/) to set up a development environment.

Once that's activated, you can easily install the project requirements:
```
pip install -r requirements.txt
```

### Prerequisites

* **pymongo>=4.1** for data storage; everything else is handled by the server.

## Usage

The server can be started by running `python server.py`

Once that's running, a client can connect to the server using `python client.py`

## License

[MIT](LICENSE)
