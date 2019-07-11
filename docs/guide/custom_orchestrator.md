# Orchestrating TFX Pipelines

## Custom Orchestrator

TFX is designed to be highly portable to multiple environments and orchestration
frameworks. Users can create their custom orchestrators other than the provided
ones, e.g., [Airflow](airflow.md) and [Kubeflow](kubeflow.md).

All orchestractors are inherited from
[TfxRunner](https://github.com/tensorflow/tfx/blob/master/tfx/orchestration/tfx_runner.py).
TFX orchestractor takes the logical pipeline object, which contains pipeline
args, components and DAG, and responsible for scheduling components of TFX
pipeline based on DAG.

Here we demonstrate how to create a custom orchestrator with
[ComponentLauncher](https://github.com/tensorflow/tfx/blob/master/tfx/orchestration/component_launcher.py).
As ComponentLauncher already handles driver, executor and publisher of a single
component. The orchestrator just need to schedule ComponentLaunchers based on
the DAG. The following example shows a simple toy orchestrator, which just run
the component one by one in DAG's topological order.

```python
import datetime

from tfx.orchestration import component_launcher
from tfx.orchestration import data_types
from tfx.orchestration import tfx_runner

class DirectRunner(tfx_runner.TfxRunner):
  """Tfx direct runner."""

  def run(self, pipeline):
    """Directly run components in topological order."""
    # Run id is needed for each run.
    pipeline.pipeline_info.run_id = datetime.datetime.now().isoformat()

    # pipeline.components are in topological order already.
    for component in pipeline.components:
      component_launcher.ComponentLauncher(
          component=component,
          pipeline_info=pipeline.pipeline_info,
          driver_args=data_types.DriverArgs(
              enable_cache=pipeline.enable_cache),
          metadata_connection_config=pipeline.metadata_connection_config,
          additional_pipeline_args=pipeline.additional_pipeline_args
      ).launch()
```

Above orchestrator can be used in python DSL:

```python
import direct_runner
from tfx.orchestration import pipeline

def _create_pipeline() -> pipeline.Pipeline:
  ...

direct_runner.DirectRunner().run(_create_pipeline())
```

To run above python DSL file (assume named dsl.py), simply do the following:

```bash
python dsl.py
```
