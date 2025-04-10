import ray
import time

class TestFlow:
    input_list = []
    def __init__(self, name):
        self.name = name

    def say_hello(self, input, round_num):
        TestFlow.input_list.append(input)
        print(f"Actor: {input}, Round: {round_num}, Testflow.input_list: {TestFlow.input_list}")
        return f"Hello from {input}!"

# Remote function that calls the method
@ray.remote
def call_method(obj, method_name, *args, **kwargs):
    method = getattr(obj, method_name)
    return method(*args, **kwargs)

def run():
    # Initialize ray
    ray.init()

    # Create an instance
    testflow = TestFlow("OpenFL")

    collaborators = ["col1", "col2", "col3", "col4"]
    for round_num in range(2):
        for site in collaborators:
            # Call the instance method remotely
            future_col = call_method.remote(testflow, "say_hello", site, round_num)
            print(ray.get(future_col))

        time.sleep(2)
