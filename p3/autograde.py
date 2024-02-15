from argparse import ArgumentParser
from functools import wraps
from os import environ
from pathlib import Path
from subprocess import DEVNULL, call, check_output
from time import sleep

import grpc
from google.protobuf.descriptor import FieldDescriptor

from tester import cleanup, init, test, tester_main

PORT = 5440
DOCKER_CONTAINER_NAME = "p3_autograde"

CWD = Path(__file__).parent


def with_client():
    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            if not (CWD / "mathdb_pb2_grpc.py").exists():
                return "mathdb_pb2_grpc.py not found"

            # pylint: disable=import-outside-toplevel,import-error
            from mathdb_pb2_grpc import MathDbStub

            # pylint: enable=import-outside-toplevel,import-error

            addr = f"127.0.0.1:{PORT}"
            channel = grpc.insecure_channel(addr)
            stub = MathDbStub(channel)
            return test_func(stub)

        return wrapper

    return decorator


def client_workload(*csv_files):
    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
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


@cleanup
def _cleanup(*_args, **_kwargs):
    call(["docker", "stop", DOCKER_CONTAINER_NAME], stdout=DEVNULL, stderr=DEVNULL)
    call(["docker", "rm", DOCKER_CONTAINER_NAME], stdout=DEVNULL, stderr=DEVNULL)


@init
def _init(*_args, **_kwargs):
    _cleanup()


@test(10)
def math_cache_ops():
    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error

    for method in ["get", "set", "add", "sub", "mul", "div"]:
        assert hasattr(MathCache, method), f"MathCache.{method} not found"

    cache = MathCache()
    cache.set("key1", 1)
    assert cache.get("key1")[0] == 1, "cache.get('key1') should return 1"
    cache.set("key1", 2)
    assert cache.get("key1")[0] == 2, "cache.get('key1') should return 2"

    cache.set("key2", 4)
    assert (
        cache.add("key1", "key2")[0] == 6
    ), "cache.add('key1', 'key2') should return 6"
    assert (
        cache.sub("key1", "key2")[0] == -2
    ), "cache.sub('key1', 'key2') should return -2"
    assert (
        cache.mul("key1", "key2")[0] == 8
    ), "cache.mul('key1', 'key2') should return 8"
    assert (
        cache.div("key1", "key2")[0] == 0.5
    ), "cache.div('key1', 'key2') should return 2"


@test(10)
def math_cache_lru_simple():
    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error

    cache = MathCache()

    for i in range(10):
        cache.set(f"key{i}", i)

    assert (
        cache.add("key1", "key2")[1] is False
    ), "cache.add('key1', 'key2') should miss cache"
    assert (
        cache.sub("key3", "key4")[1] is False
    ), "cache.sub('key3', 'key4') should miss cache"
    assert (
        cache.mul("key5", "key6")[1] is False
    ), "cache.mul('key5', 'key6') should miss cache"
    assert (
        cache.div("key7", "key8")[1] is False
    ), "cache.div('key7', 'key8') should miss cache"
    assert (
        cache.add("key9", "key10")[1] is False
    ), "cache.add('key9', 'key10') should miss cache"

    assert (
        cache.add("key1", "key2")[1] is True
    ), "cache.add('key1', 'key2') should hit cache"
    assert (
        cache.sub("key3", "key4")[1] is True
    ), "cache.sub('key3', 'key4') should hit cache"
    assert (
        cache.mul("key5", "key6")[1] is True
    ), "cache.mul('key5', 'key6') should hit cache"
    assert (
        cache.div("key7", "key8")[1] is True
    ), "cache.div('key7', 'key8') should hit cache"
    assert (
        cache.add("key9", "key10")[1] is True
    ), "cache.add('key9', 'key10') should hit cache"

    cache.set("key10", 10)
    assert (
        cache.add("key1", "key2")[1] is False
    ), "cache.add('key1', 'key2') should miss cache"
    assert (
        cache.sub("key3", "key4")[1] is False
    ), "cache.sub('key3', 'key4') should miss cache"
    assert (
        cache.mul("key5", "key6")[1] is False
    ), "cache.mul('key5', 'key6') should miss cache"
    assert (
        cache.div("key7", "key8")[1] is False
    ), "cache.div('key7', 'key8') should miss cache"
    assert (
        cache.add("key9", "key10")[1] is False
    ), "cache.add('key9', 'key10') should miss cache"


@test(10)
def math_cache_lru_complex():
    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from server import MathCache

    # pylint: enable=import-outside-toplevel,import-error

    cache = MathCache()
    for i in range(5):
        cache.set(f"key{i}", i)

    for i in range(5):
        cache.add(f"key{i}", f"key{i}")
        cache.sub(f"key{i}", f"key{i}")

    for i in range(5):
        assert (
            cache.add(f"key{i}", f"key{i}")[1] is True
        ), f"cache.add('key{i}', 'key{i}') should hit cache"
        assert (
            cache.sub(f"key{i}", f"key{i}")[1] is True
        ), f"cache.sub('key{i}', 'key{i}') should hit cache"

    for i in range(5):
        cache.mult(f"key{i}", f"key{i}")

    for i in range(5):
        assert (
            cache.add(f"key{i}", f"key{i}")[1] is False
        ), f"cache.add('key{i}', 'key{i}') should miss cache"
        assert (
            cache.sub(f"key{i}", f"key{i}")[1] is True
        ), f"cache.sub('key{i}', 'key{i}') should hit cache"
        assert (
            cache.mul(f"key{i}", f"key{i}")[1] is True
        ), f"cache.mul('key{i}', 'key{i}') should hit cache"


@test(10)
def math_db_grpc():
    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from mathdb_pb2 import (
        SetRequest,  # type: ignore
        SetResponse,  # type: ignore
        GetRequest,  # type: ignore
        GetResponse,  # type: ignore
        AddRequest,  # type: ignore
        AddResponse,  # type: ignore
        SubRequest,  # type: ignore
        SubResponse,  # type: ignore
        MulRequest,  # type: ignore
        MulResponse,  # type: ignore
        DivRequest,  # type: ignore
        DivResponse,  # type: ignore
    )

    # pylint: enable=import-outside-toplevel,import-error

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
        **{
            request.DESCRIPTOR: {
                "key_a": [
                    FieldDescriptor.LABEL_OPTIONAL,
                    FieldDescriptor.CPPTYPE_STRING,
                ],
                "key_b": [
                    FieldDescriptor.LABEL_OPTIONAL,
                    FieldDescriptor.CPPTYPE_STRING,
                ],
            }
            for request in [AddRequest, SubRequest, MulRequest, DivRequest]
        },
        **{
            response.DESCRIPTOR: {
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
            }
            for response in [AddResponse, SubResponse, MulResponse, DivResponse]
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


@test(10)
def math_db_server_simple():
    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from server import MathDb
    from mathdb_pb2 import (
        SetRequest,  # type: ignore
        SetResponse,  # type: ignore
        GetRequest,  # type: ignore
        GetResponse,  # type: ignore
        AddRequest,  # type: ignore
        AddResponse,  # type: ignore
        SubRequest,  # type: ignore
        SubResponse,  # type: ignore
        MulRequest,  # type: ignore
        MulResponse,  # type: ignore
        DivRequest,  # type: ignore
        DivResponse,  # type: ignore
    )

    # pylint: enable=import-outside-toplevel,import-error

    db = MathDb()
    response = db.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse), response
    response = db.Get(GetRequest(key="key1"))
    assert isinstance(response, GetResponse) and response.value == 1, response

    db.Set(SetRequest(key="key2", value=2))

    response = db.Add(AddRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, AddResponse) and response.value == 3, response
    response = db.Sub(SubRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, SubResponse) and response.value == -1, response
    response = db.Mul(MulRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, MulResponse) and response.value == 2, response
    response = db.Div(DivRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, DivResponse) and response.value == 0.5, response


@test(10)
def math_db_server_errors():
    if not (CWD / "server.py").exists():
        return "server.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from server import MathDb
    from mathdb_pb2 import (
        SetRequest,  # type: ignore
        SetResponse,  # type: ignore
        GetRequest,  # type: ignore
        GetResponse,  # type: ignore
        AddRequest,  # type: ignore
        AddResponse,  # type: ignore
        SubRequest,  # type: ignore
        SubResponse,  # type: ignore
        MulRequest,  # type: ignore
        MulResponse,  # type: ignore
        DivRequest,  # type: ignore
        DivResponse,  # type: ignore
    )

    # pylint: enable=import-outside-toplevel,import-error

    db = MathDb()
    response = db.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse) and response.error == "", response
    response = db.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse) and response.error == "", response
    response = db.Get(GetRequest(key="key2"))
    assert isinstance(response, GetResponse) and response.error == "", response
    response = db.Add(AddRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, AddResponse) and response.error == "", response
    response = db.Sub(SubRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, SubResponse) and response.error == "", response
    response = db.Mul(MulRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, MulResponse) and response.error == "", response
    response = db.Div(DivRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, DivResponse) and response.error == "", response

    response = db.Get(GetRequest(key="key3"))
    assert isinstance(response, GetResponse) and response.error != "", response
    response = db.Add(AddRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, AddResponse) and response.error != "", response
    response = db.Sub(SubRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, SubResponse) and response.error != "", response
    response = db.Mul(MulRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, MulResponse) and response.error != "", response
    response = db.Div(DivRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, DivResponse) and response.error != "", response

    db.Set(SetRequest(key="key3", value=0))
    response = db.Div(DivRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, DivResponse) and response.error != "", response
    response = db.Div(DivRequest(key_a="key3", key_b="key1"))
    assert isinstance(response, DivResponse) and response.error == "", response


@test(10)
def docker_build_run():
    if not (CWD / "Dockerfile").exists():
        return "Dockerfile not found"

    environment = environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"
    check_output(["docker", "build", ".", "-t", DOCKER_CONTAINER_NAME], env=environment)
    check_output(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            DOCKER_CONTAINER_NAME,
            "-w",
            "/autograde",
            "-v",
            ".:/autograde",
            DOCKER_CONTAINER_NAME,
        ]
    )
    sleep(5)  # wait for server to start


@test(5)
@with_client()
def math_db_server_simple_over_grpc(stub):
    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from mathdb_pb2 import (
        SetRequest,  # type: ignore
        SetResponse,  # type: ignore
        GetRequest,  # type: ignore
        GetResponse,  # type: ignore
        AddRequest,  # type: ignore
        AddResponse,  # type: ignore
        SubRequest,  # type: ignore
        SubResponse,  # type: ignore
        MulRequest,  # type: ignore
        MulResponse,  # type: ignore
        DivRequest,  # type: ignore
        DivResponse,  # type: ignore
    )

    # pylint: enable=import-outside-toplevel,import-error

    response = stub.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse), response
    response = stub.Get(GetRequest(key="key1"))
    assert isinstance(response, GetResponse) and response.value == 1, response

    stub.Set(SetRequest(key="key2", value=2))

    response = stub.Add(AddRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, AddResponse) and response.value == 3, response
    response = stub.Sub(SubRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, SubResponse) and response.value == -1, response
    response = stub.Mul(MulRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, MulResponse) and response.value == 2, response
    response = stub.Div(DivRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, DivResponse) and response.value == 0.5, response


@test(5)
@with_client()
def math_db_server_errors_over_grpc(stub):
    if not (CWD / "mathdb_pb2.py").exists():
        return "mathdb_pb2.py not found"

    # pylint: disable=import-outside-toplevel,import-error
    from mathdb_pb2 import (
        SetRequest,  # type: ignore
        SetResponse,  # type: ignore
        GetRequest,  # type: ignore
        GetResponse,  # type: ignore
        AddRequest,  # type: ignore
        AddResponse,  # type: ignore
        SubRequest,  # type: ignore
        SubResponse,  # type: ignore
        MulRequest,  # type: ignore
        MulResponse,  # type: ignore
        DivRequest,  # type: ignore
        DivResponse,  # type: ignore
    )

    # pylint: enable=import-outside-toplevel,import-error

    response = stub.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse) and response.error == "", response
    response = stub.Set(SetRequest(key="key1", value=1))
    assert isinstance(response, SetResponse) and response.error == "", response
    response = stub.Get(GetRequest(key="key2"))
    assert isinstance(response, GetResponse) and response.error == "", response
    response = stub.Add(AddRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, AddResponse) and response.error == "", response
    response = stub.Sub(SubRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, SubResponse) and response.error == "", response
    response = stub.Mul(MulRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, MulResponse) and response.error == "", response
    response = stub.Div(DivRequest(key_a="key1", key_b="key2"))
    assert isinstance(response, DivResponse) and response.error == "", response

    response = stub.Get(GetRequest(key="key3"))
    assert isinstance(response, GetResponse) and response.error != "", response
    response = stub.Add(AddRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, AddResponse) and response.error != "", response
    response = stub.Sub(SubRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, SubResponse) and response.error != "", response
    response = stub.Mul(MulRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, MulResponse) and response.error != "", response
    response = stub.Div(DivRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, DivResponse) and response.error != "", response

    stub.Set(SetRequest(key="key3", value=0))
    response = stub.Div(DivRequest(key_a="key1", key_b="key3"))
    assert isinstance(response, DivResponse) and response.error != "", response
    response = stub.Div(DivRequest(key_a="key3", key_b="key1"))
    assert isinstance(response, DivResponse) and response.error == "", response


@test(5)
@client_workload("workload/workload1.csv")
def client_workload_1(hit_rate):
    expected_hit_rate = 12 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5)
@client_workload("workload/workload2.csv")
def client_workload_2(hit_rate):
    expected_hit_rate = 6 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5)
@client_workload("workload/workload3.csv")
def client_workload_3(hit_rate):
    expected_hit_rate = 10 / 100
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(5)
@client_workload(
    "workload/workload1.csv", "workload/workload2.csv", "workload/workload3.csv"
)
def client_workload_combined(hit_rate):
    assert (
        0 <= hit_rate <= 1
    ), f"Expected hit rate to be between 0 and 1, but got {hit_rate}"


if __name__ == "__main__":
    parser = ArgumentParser()
    tester_main(parser)
