from concurrent import futures
import traceback
import math_pb2_grpc
import math_pb2
import grpc
import numpy as np

class MyCalc(math_pb2_grpc.CalcServicer):
    def Mult(self, request, context):
        try:
            err = ""
            print("hi")
            print(request)
        except Exception:
            err = traceback.format_exc()
        return math_pb2.MultResp(result = request.x * request.y, error=err)

    def MultMany(self, request, context):
        try:
            err = ""
            total = 1
            for num in request.nums:
                total *= num
        except Exception:
            total = np.nan
            err = traceback.format_exc()
        return math_pb2.MultResp(result = total, error = err)
    
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
# add servicer
math_pb2_grpc.add_CalcServicer_to_server(MyCalc(), server)
server.add_insecure_port('0.0.0.0:5440')
server.start()
print("started")
server.wait_for_termination()
