from concurrent import futures
import traceback

    
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
# add servicer

server.add_insecure_port('0.0.0.0:5440')
server.start()
print("started")
server.wait_for_termination()
