# P3 (6% of grade): MathDB

## [GitHub Classroom Link](https://classroom.github.com/a/9Ttkhntt)

## Overview

In this project, you will write a database to handle simple math operations (addition, subtraction, multiplication, and division). This is similar to a _key-value store_, but comes with the added functionality of the math operations.

Your database will use multiple threads and cache calculation results. This will save effort when the database is given the same inputs repeatedly.

You'll start by writing your code in a Python class and calling the methods in it. By the end, though, your database will run in a Docker container and receive requests over a network (via gRPC calls).

Learning objectives:

-   Use threads and locks correctly
-   Cache computation results with an LRU policy
-   Measure performance statistics like cache hit rate
-   Communicate between clients and servers via gRPC

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

-   2024-02-16:
    -   Protobuf typos in README fixed
    -   Add clarification on cache
    -   Dynamically import grpc
-   2024-02-19
    -   Clarify client thread-safe mechanisms
    -   Clarify client use of third-party packages
    -   Clarify symmetric cache entries

## Setup

Join the classroom and clone the provided code.

Run `setup.sh` to get all of the relevant and up-to-date autograding files.

Commit early and often to save your work!

For this assignment, we recommend either using [VSCode with SSH](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-ssh) or vim/nano via the terminal.

## Part 1: Math Cache

In `server.py`, write a class called `MathCache` with a few methods: `Set(key, value)` and `Get(key)`.

These methods will access an instance variable that is a dictionary from key to value. For now, assume that `key` is present in the dictionary during a `Get` call (we will handle errors later).

Next, add the following methods:

-   `Add(key_a, key_b)`
-   `Sub(key_a, key_b)`
-   `Mult(key_a, key_b)`
-   `Div(key_a, key_b)` (again, don't worry about errors here for now, e.g. divide by 0)

These methods will use `Get` for both keys and perform the math operation. Lastly, they will return the result and whether the cache was hit (a boolean). You will implement this last bit in the next section

At this point, try running `autograde.py`. The test `math_cache_ops` should pass.

If `key` is not present for any of the methods, simply raise a `KeyError`. (hint: this is already what is raised by `my_dict[key]`)

### Caching

Now you will add code for an LRU cache to your `MathCache` class. This cache is a separate data structure from the key-value store. Requirements:

-   Use the operation name and key names as the key to the cache. E.g., `("add", "key_a", "key_b")`.
    -   (Note that for simplicity we will not consider symmetric entries in the cache to be the same for symmtric operations like add or multiply.)

-   The cache should hold a maximum of 10 entries.
-   Whenever `Set` is called, _invalidate_ the cache (clear out all the entries in the cache). We don't expect the same results now that the key values have changed. 
    -   (Note that in the real world, you could cleverly only invalidate the entries pertaining to the updated key, but you will not have to do that for this assignment.)


At this point, try running `autograde.py`. The test `math_cache_lru_simple` and `math_cache_lru_complex` should pass.

### Locking

There will eventually be multiple threads calling methods in `MathCache` simultaneously, so you will now add a lock (again as an instance variable).

The lock should be:

-   Held when any shared data (e.g., the cache or key-value dictionary) are modified.
-   Released at the end of each call, [even if there is an exception](https://docs.python.org/3/tutorial/errors.html#defining-clean-up-actions).

## Part 2: MathDB

### Protocol

Read this guide for gRPC with Python:

-   https://grpc.io/docs/languages/python/quickstart/
-   https://grpc.io/docs/languages/python/basics/

Install the tools (be sure to upgrade pip first, as described in the directions):

```shell
pip3 install grpcio==1.60.1 grpcio-tools==1.60.1 --break-system-packages
```

Feel free to setup a virtualenv or similar if you are comfortable. Otherwise, the above is also acceptable.

Create a file called `mathdb.proto` containing a service called `MathDb`.
Specify `syntax="proto3";` at the top of your file.
`MathDb` will contain 6 RPCs. Also define the request/response message types.

1. `Set`
    - `SetRequest`: `key` (`string`), and `value` (`float`)
    - `SetResponse`: `error` (`string`)
2. `Get`
    - `GetRequest`: `key` (`string`)
    - `GetResponse`: `value` (`float`) and `error` (`string`)
3. `Add`/`Sub`/`Mult`/`Div`
    - `BinaryOpRequest`: `key_a` (`string`) and `key_b` (`string`)
    - `BinaryOpResponse`: `value` (`float`), `cache_hit` (`bool`), and `error` (`string`)

You can build your `.proto` with:

```shell
python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. mathdb.proto
```

Verify `mathdb_pb2.py` and `mathdb_pb2_grpc.py` were generated.

At this point, try running `autograde.py`. The test `math_db_grpc` should pass.

### Server

Add a `MathDb` class to `server.py` that inherits from `mathdb_pb2_grpc.MathDbServicer`. This base class has the function signatures of all the methods you will need to implement. `MathDb` should override the six methods of `MathDbServicer` and use a `MathCache` to help calculate the answers.

The `error` fields should contain the empty string `""` when no exceptions occur; however, you should wrap your code in a `try/except` so that in the event of an exception, you can return an error message that can help you debug. If you did not do this, exceptions happening on the server side would not show up anywhere, making troubleshooting difficult.

Hint: you can use `traceback.format_exc` in your `except` block to get a string representation of the exception to return

Start your server like this:

```python
import grpc
from concurrent import futures

import mathdb_pb2_grpc

if __name__ == "__main__":
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
  mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
  server.add_insecure_port("[::]:5440", )
  server.start()
  server.wait_for_termination()
```

You can do this directly in the bottom of your server.py, or within a `main` function; feel free to move imports to the top of your file if you like.

At this point, try running `autograde.py`. The test `math_db_server_simple` and `math_db_server_errors` should pass. Additionally, after building the `Dockerfile` (mentioned later), `math_db_server_simple_over_grpc` and `math_db_server_errors should pass.

## Part 3: Client

Write a gRPC client named `client.py` that can be run like this:

```bash
python3 client.py <PORT> <THREAD1-WORK.csv> <THREAD2-WORK.csv> <THREAD3-WORK.csv> ...
```

You are welcome to read `sys.argv` or implement your own [`argparse.ArgumentParser`](https://docs.python.org/3/howto/argparse.html#argparse-tutorial) if you would like.

_For this example_, your client should do the following, in order:

1. Connect to the server at port `5440`.
2. Launch three threads, each thread being responsible for one of the three CSV files.
3. Each thread should loop over the rows in its CSV files.
   - Each row will contain a command name and either one or two key names that should be used to make a call to the server.
   - The threads should then collect some aggregate statistics about the total number of hits and misses. **Be sure do this in a thread safe way!** You can either aggregate the totals for each thread independently using a global array indexed by the thread index or access a global variable if it is protected by lock!

4. The main thread should call `join` to wait until the 3 threads are finished.

Note that your client should work for any number ($n \geq 1$) of CSV files.

The client can print other stuff, but its very last line of output should be the overall hit rate. For example, if the hit counts and total counts for the three threads are 1/1, 0/1, and 3/8 respectively, then the overall hit rate would be (1+0+3) / (1+1+8) = 0.4.

At this point, try running `autograde.py`. The tests `client_workload_{1,2,3,combined}` should pass.

## Part 4: Deployment

You should write a `Dockerfile` to build an image with everything needed (Python, packages, etc.) to run both your server and client. Your Docker image should:

-   Build via `docker build -t p3 .`
-   Run via `docker run -p 127.0.0.1:5440:5440 p3` (i.e., you can map any external port to the internal port of 5440)
-   Either `python3 client.py <PORT> <THREAD1-WORK.csv> <THREAD2-WORK.csv> <THREAD3-WORK.csv> ...` or `docker exec -it <CONTAINER-NAME> python3 client.py <PORT> <PORT> <THREAD1-WORK.csv> <THREAD2-WORK.csv> <THREAD3-WORK.csv>`

You should then be able to run the client outside of the container (using any port you would like), or use a `docker exec` to enter the container and run the client with port 5440.

Your client should not use any packages outside of `numpy` or `pandas` (again, these are not necessary; an alternative such as the built-in `csv.DictReader` will suffice). If you have already included them, please let us know so that we may grade them appropriately!

## Submission

You should organize and commit your files such that we can run the code in your repo like this:

```shell
python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. mathdb.proto
python3 autograde.py
```

This should include:

-   `client.py`
-   `server.py`
-   `mathdb.proto`
-   `Dockerfile`

## Tester

Run `python autograde.py` to get your score, which contains the following tests:

1. `math_cache_ops` (10)
2. `math_cache_lru_simple` (10)
3. `math_cache_lru_complex` (10)
4. `math_db_grpc` (10)
5. `math_db_server_simple` (10)
6. `math_db_server_errors` (10)
7. `docker_build_run` (10)
    - docker image builds and can start running
8. `math_db_server_simple_over_grpc` (5)
9. `math_db_server_errors_over_grpc` (5)
10. `client_workload_1` (5)
11. `client_workload_2` (5)
12. `client_workload_3` (5)
13. `client_workload_combined` (5)
    - handles multiple CSVs

There will also be a manual grading portion, so the score you see in `test.json` may not reflect your final score.

We will check the following:

1. `math_cache_locking` (15)
    - Server uses threads and locking is correct
2. `client_thread_safe` (15)
    - Client uses threads and locking is correct
