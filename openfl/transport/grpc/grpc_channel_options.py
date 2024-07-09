# Copyright 2020-2024 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

max_metadata_size = 32 * 2**20
max_message_length = 2**30

channel_options = [
    ("grpc.max_metadata_size", max_metadata_size),
    ("grpc.max_send_message_length", max_message_length),
    ("grpc.max_receive_message_length", max_message_length),
]
