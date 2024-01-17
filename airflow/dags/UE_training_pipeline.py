import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipeline.training_pipeline import TrainingPipeline
from src.entity.config_entity import TrainingPipelineConfig

with DAG(
    'User_Experience',
    default_args={'retries':2},
    description='User Experience Project',
    schedule_interval='@weekly',
    start_date=pendulum.datetime(2023, 11, 20, tz='UTC')
) as dag:
    from src.pipeline.training_pipeline import TrainingPipeline
    from src.entity.config_entity import TrainingPipelineConfig

    training_pipeline = TrainingPipeline(TrainingPipelineConfig())

    def data_ingestion(**kwargs):
        from src.entity.config_entity import DataIngestionConfig

        data_ingestion_artifact = training_pipeline.initiate_data_ingestion(DataIngestionConfig())

        ti = kwargs['ti']

        print(data_ingestion_artifact)

        ti.xcom_push('data_ingestion_artifact', data_ingestion_artifact)

    def data_validation(**kwargs):
        from src.entity.config_entity import DataValidationConfig

        ti = kwargs['ti']

        data_ingestion_artifact = ti.xcom_pull(task_ids='data_ingestion', key='data_ingestion_artifact')

        data_validation_artifact = training_pipeline.initiate_data_validation(DataValidationConfig(), data_ingestion_artifact)

        print(data_validation_artifact)

        ti.xcom_push('data_validation_artifact', data_validation_artifact)

    def data_transformation(**kwargs):
        from src.entity.config_entity import DataTransformationConfig

        ti = kwargs['ti']

        data_validation_artifact = ti.xcom_pull(task_ids='data_validation', key='data_validation_artifact')

        data_transformation_artifact = training_pipeline.initiate_data_transformation(DataTransformationConfig(), data_validation_artifact)

        print(data_transformation_artifact)

        ti.xcom_push('data_transformation_artifact', data_transformation_artifact)

    def model_trainer(**kwargs):
        from src.entity.config_entity import ModelTrainerConfig

        ti = kwargs['ti']

        data_transformation_artifact = ti.xcom_pull(task_ids='data_transformation', key='data_transformation_artifact')

        model_trainer_artifact = training_pipeline.initiate_model_trainer(ModelTrainerConfig(), data_transformation_artifact)

        print(model_trainer_artifact)

        ti.xcom_push('model_trainer_artifact', model_trainer_artifact)

    def model_evaluation(**kwargs):
        from src.entity.config_entity import ModelEvaluatorConfig

        ti = kwargs['ti']

        data_transformation_artifact = ti.xcom_pull(task_ids='data_transformation', key='data_transformation_artifact')

        model_trainer_artifact = ti.xcom_pull(task_ids='model_trainer', key='model_trainer_artifact')

        model_evaluation_artifact = training_pipeline.initiate_model_evaluation(ModelEvaluatorConfig(), model_trainer_artifact, data_transformation_artifact)

        print(model_evaluation_artifact)

        ti.xcom_push('model_evaluation_artifact', model_evaluation_artifact)

    def model_pusher(**kwargs):
        from src.entity.config_entity import ModelPusherConfig, ModelEvaluatorConfig, ModelTrainerConfig

        ti = kwargs['ti']

        data_validation_artifact = ti.xcom_pull(task_ids='data_validation', key='data_validation_artifact')

        model_evaluation_artifact = ti.xcom_pull(task_ids='model_evaluation', key='model_evaluation_artifact')

        if model_evaluation_artifact.is_accepted == True:
            training_pipeline.initiate_model_pusher(ModelEvaluatorConfig(), ModelPusherConfig(), ModelTrainerConfig())
            print("Training completed successfully")
        else:
            print("Training model Rejected")
