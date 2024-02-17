"""Autograder for Project 3"""

import traceback
from argparse import ArgumentParser
from functools import wraps
from os import environ, linesep
from pathlib import Path
from subprocess import DEVNULL, CalledProcessError, call, check_output
from time import sleep

from tester import cleanup, init, test, tester_main

PORT = 5440
DOCKER_IMAGE_NAME = "p3_autograde"
DOCKER_CONTAINER_NAME = "p3_autograde"

CWD = Path(__file__).parent


def with_client():
    """Provides a gRPC client stub to the decorated test function"""

    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            try:
                # pylint: disable=import-outside-toplevel,import-error
                import grpc

                # pylint: enable=import-outside-toplevel,import-error
            except ImportError:
                return "grpc not installed"

            if not (CWD / "mathdb_pb2_grpc.py").exists():
                return "mathdb_pb2_grpc.py not found"

            # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
            from mathdb_pb2_grpc import MathDbStub

            # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

            addr = f"127.0.0.1:{PORT}"
            channel = grpc.insecure_channel(addr)
            stub = MathDbStub(channel)
            return test_func(stub)

        return wrapper

    return decorator


def client_workload(*csv_files):
    """Runs the client with the given workload files."""

    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            if not docker_container_is_running():
                return "Docker container is not running"

            output = (
                check_output(["python3", "client.py", str(PORT), *csv_files])
                .decode("utf-8")
                .splitlines()
            )

            assert len(output) >= 1, "Expected at least 1 line of output"

            last_line = output[-1]

            try:
                hit_rate = float(last_line)
            except ValueError:
                assert False, f"Expected last line to be a float, but got {last_line}"

            return test_func(hit_rate)

        return wrapper

    return decorator


def docker_container_is_running():
    """Checks if the Docker container is running."""

    try:
        output = check_output(
            [
                "docker",
                "inspect",
                "--type",
                "container",
                "-f",
                "{{.State.Running}}",
                DOCKER_CONTAINER_NAME,
            ],
            stderr=DEVNULL,
        )
        return output.decode("utf-8").strip() == "true"
    except CalledProcessError:
        return False


class ServerException(Exception):
    """Exception raised by the server"""

    def __init__(self, error):
        self.message = linesep.join(
            line.strip() for line in error.splitlines() if line.strip()
        )


@cleanup
def _cleanup(*_args, **_kwargs):
    print("Cleaning up...")
    call(["docker", "stop", DOCKER_CONTAINER_NAME], stdout=DEVNULL, stderr=DEVNULL)
    call(["docker", "rm", DOCKER_CONTAINER_NAME], stdout=DEVNULL, stderr=DEVNULL)
    call(["docker", "rmi", DOCKER_IMAGE_NAME], stdout=DEVNULL, stderr=DEVNULL)


@init
def _init(*_args, **_kwargs):
    _cleanup()


@test(10, timeout=10)
def math_cache_ops():
    """Tests the MathCache class operations"""

    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    for method in ["Get", "Set", "Add", "Sub", "Mult", "Div"]:
        assert hasattr(MathCache, method), f"MathCache.{method} not found"

    cache = MathCache()
    cache.Set("key1", 1)
    assert cache.Get("key1") == 1, "cache.Get('key1') should return 1"
    cache.Set("key1", 2)
    assert cache.Get("key1") == 2, "cache.Get('key1') should return 2"

    cache.Set("key2", 4)
    assert (
        cache.Add("key1", "key2")[0] == 6
    ), "cache.Add('key1', 'key2') should return 6"
    assert (
        cache.Sub("key1", "key2")[0] == -2
    ), "cache.Sub('key1', 'key2') should return -2"
    assert (
        cache.Mult("key1", "key2")[0] == 8
    ), "cache.Mult('key1', 'key2') should return 8"
    assert (
        cache.Div("key1", "key2")[0] == 0.5
    ), "cache.Div('key1', 'key2') should return 2"


@test(10, timeout=10)
def math_cache_lru_simple():
    """Tests the MathCache LRU eviction."""

    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    cache = MathCache()

    for i in range(10):
        cache.Set(f"key{i}", i)

    assert (
        cache.Add("key0", "key1")[1] is False
    ), "cache.Add('key0', 'key1') should miss cache"
    assert (
        cache.Sub("key2", "key3")[1] is False
    ), "cache.Sub('key2', 'key3') should miss cache"
    assert (
        cache.Mult("key4", "key5")[1] is False
    ), "cache.Mult('key4', 'key5') should miss cache"
    assert (
        cache.Div("key6", "key7")[1] is False
    ), "cache.Div('key6', 'key7') should miss cache"
    assert (
        cache.Add("key8", "key9")[1] is False
    ), "cache.Add('key8', 'key9') should miss cache"

    assert (
        cache.Add("key0", "key1")[1] is True
    ), "cache.Add('key0', 'key1') should hit cache"
    assert (
        cache.Sub("key2", "key3")[1] is True
    ), "cache.Sub('key2', 'key3') should hit cache"
    assert (
        cache.Mult("key4", "key5")[1] is True
    ), "cache.Mult('key4', 'key5') should hit cache"
    assert (
        cache.Div("key6", "key7")[1] is True
    ), "cache.Div('key6', 'key7') should hit cache"
    assert (
        cache.Add("key8", "key9")[1] is True
    ), "cache.Add('key8', 'key9') should hit cache"

    cache.Set("key10", 10)
    assert (
        cache.Add("key0", "key1")[1] is False
    ), "cache.Add('key0', 'key1') should miss cache"
    assert (
        cache.Sub("key2", "key3")[1] is False
    ), "cache.Sub('key2', 'key3') should miss cache"
    assert (
        cache.Mult("key4", "key5")[1] is False
    ), "cache.Mult('key4', 'key5') should miss cache"
    assert (
        cache.Div("key6", "key7")[1] is False
    ), "cache.Div('key6', 'key7') should miss cache"
    assert (
        cache.Add("key8", "key9")[1] is False
    ), "cache.Add('key8', 'key9') should miss cache"

    assert (
        cache.Add("key0", "key1")[1] is True
    ), "cache.Add('key0', 'key1') should hit cache"
    assert (
        cache.Sub("key2", "key3")[1] is True
    ), "cache.Sub('key2', 'key3') should hit cache"
    assert (
        cache.Mult("key4", "key5")[1] is True
    ), "cache.Mult('key4', 'key5') should hit cache"
    assert (
        cache.Div("key6", "key7")[1] is True
    ), "cache.Div('key6', 'key7') should hit cache"
    assert (
        cache.Add("key8", "key9")[1] is True
    ), "cache.Add('key8', 'key9') should hit cache"


@test(10, timeout=10)
def math_cache_lru_complex():
    """Tests the MathCache LRU eviction."""

    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    cache = MathCache()
    for i in range(5):
        cache.Set(f"key{i}", i)

    for i in range(5):
        cache.Add(f"key{i}", f"key{i}")
        cache.Sub(f"key{i}", f"key{i}")

    for i in range(5):
        assert (
            cache.Add(f"key{i}", f"key{i}")[1] is True
        ), f"cache.Add('key{i}', 'key{i}') should hit cache"

    for i in range(5):
        assert (
            cache.Sub(f"key{i}", f"key{i}")[1] is True
        ), f"cache.Sub('key{i}', 'key{i}') should hit cache"

    for i in range(5):
        cache.Mult(f"key{i}", f"key{i}")

    for i in range(5):
        assert (
            cache.Sub(f"key{i}", f"key{i}")[1] is True
        ), f"cache.Sub('key{i}', 'key{i}') should hit cache"

    for i in range(5):
        assert (
            cache.Add(f"key{i}", f"key{i}")[1] is False
        ), f"cache.Add('key{i}', 'key{i}') should miss cache"


@test(10, timeout=10)
def math_db_grpc():
    """Tests the mathdb.proto file and the generated Python code"""

    try:
        # pylint: disable=import-outside-toplevel,import-error
        from google.protobuf.descriptor import FieldDescriptor

        # pylint: enable=import-outside-toplevel,import-error
    except ImportError:
        return "grpc not installed"

    if not (CWD / "mathdb.proto").exists():
        return "mathdb.proto not found"

    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module,no-name-in-module
    from mathdb_pb2 import BinaryOpRequest
    from mathdb_pb2 import BinaryOpResponse
    from mathdb_pb2 import GetRequest
    from mathdb_pb2 import GetResponse
    from mathdb_pb2 import SetRequest
    from mathdb_pb2 import SetResponse

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module,no-name-in-module

    descriptors = {
        SetRequest.DESCRIPTOR: {
            "key": [FieldDescriptor.LABEL_OPTIONAL, FieldDescriptor.CPPTYPE_STRING],
            "value": [FieldDescriptor.LABEL_OPTIONAL, FieldDescriptor.CPPTYPE_FLOAT],
        },
        SetResponse.DESCRIPTOR: {
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
        GetRequest.DESCRIPTOR: {
            "key": [FieldDescriptor.LABEL_OPTIONAL, FieldDescriptor.CPPTYPE_STRING],
        },
        GetResponse.DESCRIPTOR: {
            "value": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
        BinaryOpRequest.DESCRIPTOR: {
            "key_a": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
            "key_b": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
        BinaryOpResponse.DESCRIPTOR: {
            "value": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
            "cache_hit": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_BOOL,
            ],
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
    }

    for descriptor, fields in descriptors.items():
        assert len(descriptor.fields_by_name) == len(fields), (
            f"{descriptor.name} should have {len(fields)} fields, "
            f"but it has {len(descriptor.fields_by_name)} fields"
        )

        for field_name, field_info in fields.items():
            field = descriptor.fields_by_name.get(field_name)
            assert (
                field is not None
            ), f"{descriptor.name} should have a field named {field_name}"
            assert (
                field.label == field_info[0]
            ), f"{field_name} field should be {field_info[0]}"
            assert (
                field.cpp_type == field_info[1]
            ), f"{field_name} field should be {field_info[1]}"


@test(10, timeout=10)
def math_db_server_simple():
    """Tests the MathDb server."""

    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from mathdb_pb2 import BinaryOpRequest
    from mathdb_pb2 import BinaryOpResponse
    from mathdb_pb2 import GetRequest
    from mathdb_pb2 import GetResponse
    from mathdb_pb2 import SetRequest
    from mathdb_pb2 import SetResponse

    from server import MathDb

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    db = MathDb()
    response = db.Set(SetRequest(key="key1", value=1), None)
    assert isinstance(response, SetResponse), response
    response = db.Get(GetRequest(key="key1"), None)
    assert isinstance(response, GetResponse) and response.value == 1, response

    db.Set(SetRequest(key="key2", value=2), None)

    response = db.Add(BinaryOpRequest(key_a="key1", key_b="key2"), None)
    assert isinstance(response, BinaryOpResponse) and response.value == 3, response
    response = db.Sub(BinaryOpRequest(key_a="key1", key_b="key2"), None)
    assert isinstance(response, BinaryOpResponse) and response.value == -1, response
    response = db.Mult(BinaryOpRequest(key_a="key1", key_b="key2"), None)
    assert isinstance(response, BinaryOpResponse) and response.value == 2, response
    response = db.Div(BinaryOpRequest(key_a="key1", key_b="key2"), None)
    assert isinstance(response, BinaryOpResponse) and response.value == 0.5, response


@test(10, timeout=10)
def math_db_server_errors():
    """Tests the MathDb server error handling."""

    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from mathdb_pb2 import BinaryOpRequest
    from mathdb_pb2 import BinaryOpResponse
    from mathdb_pb2 import GetRequest
    from mathdb_pb2 import GetResponse
    from mathdb_pb2 import SetRequest
    from mathdb_pb2 import SetResponse

    from server import MathDb

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    try:
        db = MathDb()
        response = db.Set(SetRequest(key="key1", value=1), None)
        assert isinstance(response, SetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Set(SetRequest(key="key2", value=2), None)
        assert isinstance(response, SetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Get(GetRequest(key="key1"), None)
        if response.error:
            raise ServerException(response.error)
        response = db.Get(GetRequest(key="key2"), None)
        assert isinstance(response, GetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Add(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Sub(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Mult(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = db.Div(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)

        response = db.Get(GetRequest(key="key3"), None)
        assert isinstance(response, GetResponse) and response.error != "", response
        response = db.Add(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = db.Sub(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = db.Mult(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = db.Div(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response

        db.Set(SetRequest(key="key3", value=0), None)
        response = db.Div(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = db.Div(BinaryOpRequest(key_a="key3", key_b="key1"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)

    except ServerException:
        message = traceback.format_exc()
        return message


@test(10, timeout=600)
def docker_build_run():
    """Builds and runs the Docker container."""

    try:
        # pylint: disable=import-outside-toplevel,import-error
        import grpc

        # pylint: enable=import-outside-toplevel,import-error
    except ImportError:
        return "grpc not installed"

    if not (CWD / "Dockerfile").exists():
        return "Dockerfile not found"

    environment = environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"
    check_output(["docker", "build", ".", "-t", DOCKER_IMAGE_NAME], env=environment)
    check_output(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            DOCKER_CONTAINER_NAME,
            "-p",
            f"{PORT}:{PORT}",
            DOCKER_IMAGE_NAME,
        ]
    )

    addr = f"127.0.0.1:{PORT}"
    channel = grpc.insecure_channel(addr)

    connected = False

    def callback(state):
        nonlocal connected
        connected = state == grpc.ChannelConnectivity.READY

    channel.subscribe(callback, try_to_connect=True)

    print("Waiting for server to start...", end="", flush=True)
    for i in range(100):
        if connected:
            print(f"{linesep}Server started")
            break
        if i % 2 == 0:
            print(".", end="", flush=True)
        sleep(0.1)
    else:
        assert False, "Server did not start"


@test(5, timeout=10)
@with_client()
def math_db_server_simple_over_grpc(stub):
    """Tests the MathDb server over gRPC."""

    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    if not docker_container_is_running():
        return "Docker container is not running"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from mathdb_pb2 import BinaryOpRequest
    from mathdb_pb2 import BinaryOpResponse
    from mathdb_pb2 import GetRequest
    from mathdb_pb2 import GetResponse
    from mathdb_pb2 import SetRequest
    from mathdb_pb2 import SetResponse

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    response = stub.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse), response
    response = stub.Get(GetRequest(key="key1"))
    assert isinstance(response, GetResponse) and response.value == 1, response

    stub.Set(SetRequest(key="key2", value=2))

    response = stub.Add(BinaryOpRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, BinaryOpResponse) and response.value == 3, response
    response = stub.Sub(BinaryOpRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, BinaryOpResponse) and response.value == -1, response
    response = stub.Mult(BinaryOpRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, BinaryOpResponse) and response.value == 2, response
    response = stub.Div(BinaryOpRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, BinaryOpResponse) and response.value == 0.5, response


@test(5, timeout=10)
@with_client()
def math_db_server_errors_over_grpc(stub):
    """Tests the MathDb server error handling over gRPC."""

    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    if not docker_container_is_running():
        return "Docker container is not running"

    # pylint: disable=import-outside-toplevel,import-error,no-name-in-module
    from mathdb_pb2 import BinaryOpRequest
    from mathdb_pb2 import BinaryOpResponse
    from mathdb_pb2 import GetRequest
    from mathdb_pb2 import GetResponse
    from mathdb_pb2 import SetRequest
    from mathdb_pb2 import SetResponse

    # pylint: enable=import-outside-toplevel,import-error,no-name-in-module

    try:
        response = stub.Set(SetRequest(key="key1", value=1), None)
        assert isinstance(response, SetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Set(SetRequest(key="key2", value=2), None)
        assert isinstance(response, SetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Get(GetRequest(key="key1"), None)
        if response.error:
            raise ServerException(response.error)
        response = stub.Get(GetRequest(key="key2"), None)
        assert isinstance(response, GetResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Add(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Sub(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Mult(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)
        response = stub.Div(BinaryOpRequest(key_a="key1", key_b="key2"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)

        response = stub.Get(GetRequest(key="key3"), None)
        assert isinstance(response, GetResponse) and response.error != "", response
        response = stub.Add(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = stub.Sub(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = stub.Mult(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = stub.Div(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response

        stub.Set(SetRequest(key="key3", value=0), None)
        response = stub.Div(BinaryOpRequest(key_a="key1", key_b="key3"), None)
        assert isinstance(response, BinaryOpResponse) and response.error != "", response
        response = stub.Div(BinaryOpRequest(key_a="key3", key_b="key1"), None)
        assert isinstance(response, BinaryOpResponse), response
        if response.error:
            raise ServerException(response.error)

    except ServerException:
        message = traceback.format_exc()
        return message


@test(5, timeout=10)
@client_workload("workload/workload1.csv")
def client_workload_1(hit_rate):
    """Tests the client with workload1.csv."""

    expected_hit_rate = 2 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5, timeout=10)
@client_workload("workload/workload2.csv")
def client_workload_2(hit_rate):
    """Tests the client with workload2.csv."""

    expected_hit_rate = 1 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5, timeout=10)
@client_workload("workload/workload3.csv")
def client_workload_3(hit_rate):
    """Tests the client with workload3.csv."""

    expected_hit_rate = 0 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5, timeout=10)
@client_workload(
    "workload/workload1.csv", "workload/workload2.csv", "workload/workload3.csv"
)
def client_workload_combined(hit_rate):
    """Tests the client with all workloads combined."""

    assert (
        0 <= hit_rate <= 1
    ), f"Expected hit rate to be between 0 and 1, but got {hit_rate}"


if __name__ == "__main__":
    parser = ArgumentParser()
    tester_main(
        parser, required_files=["Dockerfile", "client.py", "mathdb.proto", "server.py"]
    )
