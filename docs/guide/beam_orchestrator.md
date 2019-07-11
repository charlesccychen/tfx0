# Orchestrating TFX Pipelines

## Beam as Orchestrator

Several TFX components rely on [Beam](beam.md) for data processing. Other than
that, TFX can also uses
[Beam as an orchestrator](https://github.com/tensorflow/tfx/blob/master/tfx/orchestration/beam/beam_runner.py)
for executing the pipeline DAG. Beam orchestrator use a different
[BeamRunner](https://beam.apache.org/documentation/runners/capability-matrix/)
than the one for components's data processing. With default
[DirectRunner](https://beam.apache.org/documentation/runners/direct/) setup,
Beam orchestrator can be used for local debugging without extra Airflow or
Kubeflow dependencies.

See the
[TFX example on Beam](https://github.com/tensorflow/tfx/blob/master/tfx/examples/chicago_taxi_pipeline/taxi_pipeline_beam.py)
for details.
