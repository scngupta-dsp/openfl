from openfl.experimental.workflow.interface import FLSpec, Aggregator, Collaborator
from openfl.experimental.workflow.runtime import LocalRuntime
from openfl.experimental.workflow.placement import aggregator, collaborator

from copy import deepcopy
import ray
import numpy as np
import os


# Empty flow to check GaNDLF data loader issue
class FeTSFederatedFlow(FLSpec):
    def __init__(self, rounds=5, **kwargs):
        super().__init__(**kwargs)
        self.current_round = 0
        self.n_rounds = rounds

    @aggregator
    def start(self):
        self.collaborators = self.runtime.collaborators
        print(f"*********** Checking checkpoint is accessible in aggregator")
        print(f"checkpoint_folder: {self.checkpoint_folder}")
        self.model = np.zeros(1024, dtype=np.int8)

        # update checkpoint_folder
        self.checkpoint_folder = "./tmp/"

        self.next(self.fetch_parameters_for_colls)

    @aggregator
    def fetch_parameters_for_colls(self):
        print("*" * 40)
        print("Starting round  {}".format(self.current_round))
        print("*" * 40)

        ########## make a checkpoint
        filename = "model.npy"
        # Ensure the checkpoint folder exists
        if not os.path.exists(self.checkpoint_folder):
            os.makedirs(self.checkpoint_folder)

        # Construct the full path for the file
        file_path = os.path.join(self.checkpoint_folder, filename)

        # Save the numpy array to the specified path
        np.save(file_path, self.model)
        print(f"Model saved to {file_path}")

        self.next(self.aggregated_model_validation, foreach="collaborators")

    @collaborator
    def aggregated_model_validation(self):
        print(
            f"*********** Checking GaNDLF Train and Test Loaders are accessible in collaborators"
        )
        print(
            f"Col: {self.input}, train_loader: {self.train_loader}, test_loader: {self.test_loader}"
        )
        print(f"Performing aggregated model validation for collaborator {self.input}")
        self.next(self.train)

    @collaborator
    def train(self):
        print(f"Performing training for collaborator {self.input}")
        self.next(self.local_model_validation)

    @collaborator
    def local_model_validation(self):
        print(f"Performing local model validation for collaborator {self.input}")
        self.next(self.join)

    @aggregator
    def join(self, inputs):
        print(f"checkpoint_folder: {self.checkpoint_folder}")
        self.next(self.internal_loop)

    @aggregator
    def internal_loop(self):
        if self.current_round >= self.n_rounds:
            print("************* EXPERIMENT COMPLETED *************")
            print("Experiment results:")
            print("************************************************")
            self.next(self.end)
        else:
            self.current_round += 1
            self.next(self.fetch_parameters_for_colls)

    @aggregator
    def end(self):
        print("********************************")
        print("End of flow")
        print("********************************")


def callable_to_initialize_collaborator_private_attributes():
    return {"train_loader": None, "test_loader": None}


from openfl.databases import TensorDB
from threading import Lock


def callable_to_initialize_aggregator_private_attributes():
    return {"checkpoint_folder": None}


if __name__ == "__main__":

    # Setup participants
    aggregator_obj = Aggregator(
        name="agg",
        private_attributes_callable=callable_to_initialize_aggregator_private_attributes,
    )

    # Setup Collaborators private attributes via callable function
    collaborator_names = ["col1", "col2"]
    collaborator_objs = []
    for idx, collaborator_name in enumerate(collaborator_names):
        collaborator_objs.append(
            Collaborator(
                name=collaborator_name,
                num_cpus=0,
                num_gpus=0,
                private_attributes_callable=callable_to_initialize_collaborator_private_attributes,
            )
        )

    local_runtime = LocalRuntime(
        aggregator=aggregator_obj,
        collaborators=collaborator_objs,
        backend="ray",
        num_actors=3,
    )
    print(f"Local runtime collaborators = {local_runtime.collaborators}")

    flflow = FeTSFederatedFlow(rounds=1)
    flflow.runtime = local_runtime
    flflow.run()

    print(f" ******* End of experiment ******")
    print(
        f" Checkpoint saved in: {aggregator_obj.private_attributes['checkpoint_folder']}"
    )
