import numpy as np
class CustomClass:
    def __init__(self, array_size):
        self.array = np.zeros(array_size, dtype=np.int8)
    
    def print_array_size(self):
        print(f"Array size: {self.array.nbytes/(1024*1024)} MB")
    
    def print_array_shape(self):
        print(f"Array shape: {self.array.nbytes/(1024*1024)} MB")
