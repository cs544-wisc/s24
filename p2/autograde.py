import argparse

from tester import init, test, tester_main
import nbutils

ANSWERS = {}

@init
def collect_cells():
    global ANSWERS
    ANSWERS = nbutils.collect_answers("nb/p2.ipynb")

@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    outputs = ANSWERS[1]
    output = nbutils.parse_int_output(outputs)
    if not nbutils.compare_int(83520, output):
        return "Wrong answer"

@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    outputs = ANSWERS[2]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(0.0, output):
        return "Wrong answer"
    
@test(points=10)
def q3():
    if not 3 in ANSWERS:
        raise Exception("Answer to question 3 not found")
    outputs = ANSWERS[3]
    output = nbutils.parse_bool_output(outputs)
    if not nbutils.compare_bool(False, output):
        return "Wrong answer"

@test(points=10)
def q4():
    if not 4 in ANSWERS:
        raise Exception("Answer to question 4 not found")
    outputs = ANSWERS[4]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(9.844, output):
        return "Wrong answer"

@test(points=10)
def q5():
    if not 5 in ANSWERS:
        raise Exception("Answer to question 5 not found")
    outputs = ANSWERS[5]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(12.073632183908048, output):
        return "Wrong answer"

@test(points=10)
def q6():
    if not 6 in ANSWERS:
        raise Exception("Answer to question 6 not found")
    outputs = ANSWERS[6]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(19.0, output):
        return "Wrong answer"
    
@test(points=10)
def q7():
    if not 7 in ANSWERS:
        raise Exception("Answer to question 7 not found")
    outputs = ANSWERS[7]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(3.999999523162842, output):
        return "Wrong answer"
    
@test(points=10)
def q8():
    if not 8 in ANSWERS:
        raise Exception("Answer to question 8 not found")
    outputs = ANSWERS[8]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(197.8007662835249, output):
        return "Wrong answer"
    
@test(points=10)
def q9():
    if not 9 in ANSWERS:
        raise Exception("Answer to question 9 not found")
    outputs = ANSWERS[9]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(26.8113940147193, output):
        return "Wrong answer"

@test(points=10)
def q10():
    if not 10 in ANSWERS:
        raise Exception("Answer to question 10 not found")
    outputs = ANSWERS[10]
    output = nbutils.parse_float_output(outputs)
    if not nbutils.compare_float(29.05854692548551, output):
        return "Wrong answer"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    tester_main(parser)