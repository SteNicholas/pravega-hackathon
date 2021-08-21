from ai_flow import init_ai_flow_context
from ai_flow.api.ops import action_on_model_version_event
from ai_flow.model_center.entity.model_version_stage import ModelVersionEventType
from ai_flow.workflow.status import Status
from ai_flow_plugins.job_plugins.flink import set_flink_env, FlinkStreamEnv

from pravega_executor import *


def run_pravega_project():
    af.current_graph().clear_graph()
    init_ai_flow_context()
    set_flink_env(FlinkStreamEnv())
    project_name = af.current_project_config().get_project_name()
    artifact_prefix = project_name + "."
    with af.job_config('train_job'):
        train_model = af.register_model(model_name=artifact_prefix + 'pravega_model')
        af.train(input=[], training_processor=TrainModel(), model_info=train_model)
    with af.job_config('predict_job'):
        predict_input_dataset = af.register_dataset(name=artifact_prefix + 'predict_input_dataset',
                                                    uri='pravega_input',
                                                    data_format='csv')
        predict_read_dataset = af.read_dataset(dataset_info=predict_input_dataset,
                                               read_dataset_processor=Source())
        predict_channel = af.predict(input=predict_read_dataset,
                                     model_info=train_model,
                                     prediction_processor=Transformer())
        write_dataset_op = af.register_dataset(name=artifact_prefix + 'predict_output_dataset',
                                               uri='pravega_output',
                                               data_format='csv')
        af.write_dataset(input=predict_channel,
                         dataset_info=write_dataset_op,
                         write_dataset_processor=Sink())

    action_on_model_version_event(job_name='predict_job',
                                  model_name=artifact_prefix + 'pravega_model',
                                  model_version_event_type=ModelVersionEventType.MODEL_GENERATED)

    workflow_name = af.current_workflow_config().workflow_name
    stop_workflow_executions(workflow_name)
    af.workflow_operation.submit_workflow(workflow_name)
    af.workflow_operation.start_new_workflow_execution(workflow_name)


def stop_workflow_executions(workflow_name):
    workflow_executions = af.workflow_operation.list_workflow_executions(workflow_name)
    for workflow_execution in workflow_executions:
        if workflow_execution.status == Status.RUNNING:
            af.workflow_operation.stop_workflow_execution(workflow_execution.workflow_execution_id)


if __name__ == '__main__':
    run_pravega_project()
