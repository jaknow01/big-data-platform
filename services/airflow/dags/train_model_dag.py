from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='train_model',
    default_args=default_args,
    description='Train model and register it in mlflow',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mlflow', 'delta', 'sklearn']
)
def train_model_dag():
    
    @task(task_id="read_data_from_delta")
    def read_data():
        from deltalake import DeltaTable
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin123",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true"
        }

        data_path = "s3://datalake/silver/housing"
        table = DeltaTable(data_path, storage_options=storage_options)
        
        housing_df = table.to_pandas()
        print(f"Wczytano {len(housing_df)} wierszy")

        return housing_df.to_json()
    
    @task(task_id="train_and_register_model")
    def train_and_register(data_json):
        import mlflow
        import os
        import pandas as pd
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        
        MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
        os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin123"
        
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("automated_training")
        
        data = pd.read_json(data_json)
        
        with mlflow.start_run() as run:
            X = data.drop(columns=["price"])
            y = data["price"]
            
            X = pd.get_dummies(X, drop_first=True)
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            print(f"Zbiór treningowy: {len(X_train)} próbek")
            print(f"Zbiór testowy: {len(X_test)} próbek")
            print(f"Liczba cech: {X_train.shape[1]}")
            
            model = LinearRegression()
            model.fit(X_train, y_train)
            
            train_score = model.score(X_train, y_train)
            test_score = model.score(X_test, y_test)
            
            print(f"Train R² score: {train_score:.4f}")
            print(f"Test R² score: {test_score:.4f}")
            
            # Logowanie metryk
            mlflow.log_metric("train_r2_score", train_score)
            mlflow.log_metric("test_r2_score", test_score)
            mlflow.log_param("test_size", 0.2)
            mlflow.log_param("random_state", 42)

            signature = mlflow.models.infer_signature(X_train, model.predict(X_train))
            
            print("Rejestruje model")
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name="Housing_data_price_prediction",
                signature=signature
            )
            
            print(f"Model zarejestrowany w run_id: {run.info.run_id}")
            
            return run.info.run_id
    
    data = read_data()
    train_and_register(data)

train_model_dag_instance = train_model_dag()


