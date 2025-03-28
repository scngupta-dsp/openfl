import functools
import ray

# Decorator 
def test(f):
    print(f"{f.__name__} registered")
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        wrapper.some_attribute = 42
        f(*args, **kwargs)
        return wrapper
    return wrapper

# Sample function: to be executed in a remote process
@test
def sample_func():
    print("This is a test function")

# Utility function
def run(custom_func: callable = None):
    ray.init()
    print(f"{custom_func.__module__=}")
    custom_func.__module__ = "global"
    print(f"{custom_func.__module__=}")
    remote_actor = ray.remote(custom_func)
    sample_out = ray.get(remote_actor.remote())

    # Check if the attribute is available
    print(f"{sample_out.some_attribute=}")
