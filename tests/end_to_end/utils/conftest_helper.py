# Copyright 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

import argparse
import sys

from tests.end_to_end.utils.logger import logger as log


def parse_arguments():
    """
    Parse command line arguments to provide the required parameters for running the tests.

    Returns:
        argparse.Namespace: Parsed command line arguments with the following attributes:
            - results_dir (str, optional): Directory to store the results
            - num_collaborators (int, default=2): Number of collaborators
            - num_rounds (int, default=5): Number of rounds to train
            - model_name (str, default="torch_cnn_mnist"): Model name

    Raises:
        SystemExit: If the required arguments are not provided or if any argument parsing error occurs.
    """
    try:
        parser = argparse.ArgumentParser(description="Provide the required arguments to run the tests")
        parser.add_argument("--results_dir", type=str, required=False, help="Directory to store the results")
        parser.add_argument("--num_collaborators", type=int, default=2, help="Number of collaborators")
        parser.add_argument("--num_rounds", type=int, default=5, help="Number of rounds to train")
        parser.add_argument("--model_name", type=str, default="torch_cnn_mnist", help="Model name")
        args = parser.parse_known_args()[0]
        return args

    except Exception as e:
        log.error(f"Failed to parse arguments: {e}")
        sys.exit(1)
