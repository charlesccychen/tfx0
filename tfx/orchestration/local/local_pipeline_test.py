# Lint as: python2, python3
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for tfx.orchestration.beam.beam_dag_runner."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import os
import tempfile
from typing import Any, List, Text

import absl.testing.absltest

from tfx import types
from tfx.dsl.component.experimental.decorators import component
from tfx.dsl.component.experimental.annotations import InputArtifact
from tfx.dsl.component.experimental.annotations import OutputArtifact
from tfx.dsl.component.experimental.annotations import OutputDict
from tfx.dsl.component.experimental.annotations import Parameter
from tfx.dsl.io import fileio
from tfx.orchestration import pipeline
from tfx.orchestration.local import local_dag_runner
from tfx.orchestration.metadata import sqlite_metadata_connection_config


class MyDataset(types.Artifact):
  TYPE_NAME = 'MyDataset'

  def read(self) -> List[Any]:
    with fileio.open(os.path.join(self.uri, 'dataset.json')) as f:
      return json.load(f)

  def write(self, data: List[Any]):
    with fileio.open(os.path.join(self.uri, 'dataset.json'), 'w') as f:
      json.dump(data, f)


class MyModel(types.Artifact):
  TYPE_NAME = 'MyModel'

  def read(self) -> 'SimpleModel':
    return SimpleModel.read_from(self.uri)

  def write(self, model_obj: 'SimpleModel') -> None:
    model_obj.write_to(self.uri)


@component
def MyLoadDatasetComponent(dataset: OutputArtifact[MyDataset]):
  dataset.write(['A', 'B', 'C', 'C', 'C'])
  LocalDagRunnerTest.RAN_COMPONENTS.append('Load')

class SimpleModel(object):
  """Simple model that always predicts a set prediction."""

  def __init__(self, always_predict: Any):
    self.always_predict = always_predict

  @classmethod
  def read_from(cls, model_uri: Text) -> 'SimpleModel':
    with fileio.open(os.path.join(model_uri, 'model_data.json')) as f:
      data = json.load(f)
    return cls(data['prediction'])

  def write_to(self, model_uri: Text) -> None:
    data = {
      'prediction': self.always_predict
    }
    with fileio.open(os.path.join(model_uri, 'model_data.json'), 'w') as f:
      json.dump(data, f)


def train_model(records, unused_num_iterations):
  seen_count = collections.defaultdict(int)
  most_seen_count = 0
  most_seen_record = None
  for record in records:
    seen_count[record] += 1
    if seen_count[record] > most_seen_count:
      most_seen_count = seen_count[record]
      most_seen_record = record
  return SimpleModel(most_seen_record), 0.12345, most_seen_count / len(records)


@component
def MyTrainComponent(
    training_data: InputArtifact[MyDataset],
    model: OutputArtifact[MyModel],
    num_iterations: Parameter[int] = 10
    ) -> OutputDict(loss=float, accuracy=float):
  '''Simple fake trainer component.'''

  records = training_data.read()
  model_obj, loss, accuracy = train_model(records, num_iterations)
  model.write(model_obj)

  LocalDagRunnerTest.RAN_COMPONENTS.append('Train')

  return {
    'loss': loss,
    'accuracy': accuracy,
  }


@component
def MyValidateComponent(
    model: InputArtifact[MyModel],
    loss: float,
    accuracy: float
):
  """Validation component for fake trained model."""
  prediction = model.read().always_predict
  assert prediction == 'C', prediction
  assert loss == 0.12345, loss
  assert accuracy == 0.6, accuracy

  LocalDagRunnerTest.RAN_COMPONENTS.append('Validate')

class LocalDagRunnerTest(absl.testing.absltest.TestCase):

  # Global list of components names that have run, used to confirm
  # execution side-effects in local test.
  RAN_COMPONENTS = []

  def setUp(self):
    self.__class__.RAN_COMPONENTS = []

  def testSimplePipelineRun(self):
    self.assertEqual(self.RAN_COMPONENTS, [])

    # Construct component instances.
    load_component = MyLoadDatasetComponent()
    dataset = load_component.outputs['dataset']
    train_component = MyTrainComponent(
        training_data=dataset, num_iterations=5)
    validate_component = MyValidateComponent(
        model=train_component.outputs['model'],
        loss=train_component.outputs['loss'],
        accuracy=train_component.outputs['accuracy'])

    # Construct and run pipeline
    temp_path = tempfile.mkdtemp()
    pipeline_root_path = os.path.join(temp_path, 'pipeline_root')
    metadata_path = os.path.join(temp_path, 'metadata.db')
    test_pipeline = pipeline.Pipeline(
        pipeline_name='test_pipeline',
        pipeline_root=pipeline_root_path,
        metadata_connection_config=sqlite_metadata_connection_config(
            metadata_path),
        components=[
            load_component, train_component, validate_component,
        ])
    local_dag_runner.LocalDagRunner().run(test_pipeline)

    self.assertEqual(self.RAN_COMPONENTS, ['Load', 'Train', 'Validate'])


if __name__ == '__main__':
  absl.testing.absltest.main()
