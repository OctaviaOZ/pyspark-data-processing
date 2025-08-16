# PySpark Coding Challenge

This repository contains a scalable PySpark pipeline designed to process raw user interaction data and generate a feature-rich training set for a machine learning model. The entire application is containerized with Docker and orchestrated with Docker Compose for simple, reproducible execution.

## Table of Contents

- [Project Structure](#project-structure)
- [High-Level Design](#high-level-design)
  - [Training Input Structure](#training-input-structure)
  - [PyTorch Training Workflow](#pytorch-training-workflow)
  - [Data Pipeline](#data-pipeline)
  - [Scalability and Performance](#scalability-and-performance)
- [How to Run the Project](#how-to-run-the-project)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Running Tests](#running-tests)

## Project Structure

-   `data/`: (Must be created by user) Directory for input and output data.
-   `src/`: Contains the modular source code for the PySpark pipeline.
    -   `main.py`: The entry point for the Spark application.
    -   `pipeline.py`: Orchestrates the high-level flow of the pipeline.
    -   `readers.py`: Functions for reading raw data sources.
    -   `transformers.py`: Core data transformation and feature engineering logic.
    -   `schemas.py`: Defines the explicit schemas for all input data.
    -   `utils.py`: Utility functions, including the SparkSession builder.
-   `tests/`: Contains unit and integration tests for the project using pytest.
-   `Dockerfile`: Defines the container environment for the Spark application.
-   `docker-compose.yml`: Orchestrates the Spark cluster and the processing job.
-   `start.sh`: A simple script to launch the entire pipeline.
-   `pyproject.toml`: Project dependencies managed by Poetry.

## High-Level Design

### Training Input Structure

The final training data is structured in a "wide" format, where each row contains all the information needed for a single training example. This design minimizes I/O during model training, allowing for maximum GPU utilization.

-   **Schema**: `customer_id`, `item_id`, `is_order`, `actions` (array of 1000 item IDs), `action_types` (array of 1000 action types).
-   **Format**: The data is saved in **Parquet** format, partitioned by `dt` (date). Parquet's columnar storage is highly efficient for Spark and deep learning frameworks.

### PyTorch Training Workflow

This section describes how the generated training data is intended to be used in a PyTorch environment.

The training process will iterate through the 14 days of data, one day at a time, for multiple epochs. For each day:

1.  **Data Loading**: The corresponding Parquet partition (e.g., `output/dt=2025-08-16/`) will be loaded into memory.
2.  **PyTorch Dataset**: A custom PyTorch `Dataset` will be implemented. Its `__getitem__` method will be extremely fast, as it only needs to retrieve a single pre-processed row and convert its columns into PyTorch Tensors.
3.  **DataLoader and Sampling**: A PyTorch `DataLoader` will wrap the `Dataset`, with `shuffle=True` to handle the random sampling of all impressions within that day.
4.  **Training Loop**: The `DataLoader` will produce batches that can be directly fed into the model's `forward(self, impressions: Tensor, actions: Tensor, action_types: Tensor)` method.

This pre-computation strategy is key to performance. The expensive task of finding and padding the last 1000 actions for each customer is done once in Spark. This frees the training script's CPU from heavy lifting, preventing data loading from becoming a bottleneck and ensuring the GPU remains fully utilized.

### Data Pipeline

The PySpark pipeline consists of the following modular steps:

1.  **Read Data**: Ingestion functions in `readers.py` load the raw data using predefined, explicit schemas from `schemas.py`.
2.  **Unify Actions**: A transformer in `transformers.py` combines `clicks`, `add_to_carts`, and `previous_orders` into a single DataFrame, assigning a unique integer to each action type (`1`: click, `2`: add-to-cart, `3`: order).
3.  **Get Customer Actions**: For each customer, it efficiently finds the last 1000 actions that occurred *before* the `process_date` using a window function. Sequences are padded with zeros if they have fewer than 1000 actions.
4.  **Process Impressions**: Filters impressions for the given `process_date` and explodes the `impressions` array so that each row represents a single impression.
5.  **Join and Finalize**: The `pipeline.py` orchestrator joins the processed impressions with the customer action sequences on `customer_id` and handles any null values to create the final training DataFrame.

### Scalability and Performance

-   **Explicit Schemas**: Defining schemas upfront in `schemas.py` avoids slow and error-prone schema inference at runtime.
-   **Native Spark Functions**: The pipeline avoids Python UDFs in favor of highly optimized, native Spark functions for all transformations, including the padding of action sequences. This prevents performance bottlenecks caused by data serialization between the JVM and Python.
-   **Caching**: The unified `actions` DataFrame is cached in memory (`.cache()`) because it is used in the subsequent step to build customer action histories. This prevents Spark from having to recompute it from scratch.
-   **Partitioning**: The output data is partitioned by date (`dt`), allowing training jobs to efficiently select specific date ranges without scanning the entire dataset.
-   **Containerization**: Docker ensures the pipeline runs in a consistent and isolated environment, making it scalable and portable across different systems.

## How to Run the Project

The entire application is containerized, so you only need Docker to run it.

### Prerequisites

-   Docker
-   Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd pyspark-coding-challenge
    ```

2.  **Create the data directory and add input files:**
    ```bash
    mkdir -p data/impressions data/clicks data/add_to_carts data/previous_orders
    ```
    Place your raw data files into the corresponding subdirectories (e.g., `data/impressions/impressions.json`).

### Running the Pipeline

Simply run the startup script. It will build the Docker image, start the Spark cluster, run the job, and then shut everything down.

```bash
./start.sh

Of course. I have reviewed your `readme.md` file against the project's source code and the coding challenge requirements.

Overall, your `readme.md` is very well-written, clear, and professional. It correctly explains the project's purpose and high-level design. I have a few suggestions that will make it even better by enhancing its clarity, accuracy, and comprehensiveness.

-----

### `readme.md` (Revised Version)

Here is the revised version of your README with the suggested improvements incorporated.

````markdown
# PySpark Coding Challenge

This repository contains a scalable PySpark pipeline designed to process raw user interaction data and generate a feature-rich training set for a machine learning model. The entire application is containerized with Docker and orchestrated with Docker Compose for simple, reproducible execution.

## Table of Contents

- [Project Structure](#project-structure)
- [High-Level Design](#high-level-design)
  - [Training Input Structure](#training-input-structure)
  - [PyTorch Training Workflow](#pytorch-training-workflow)
  - [Data Pipeline](#data-pipeline)
  - [Scalability and Performance](#scalability-and-performance)
- [How to Run the Project](#how-to-run-the-project)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Running Tests](#running-tests)

## Project Structure

-   `data/`: (Must be created by user) Directory for input and output data.
-   `src/`: Contains the modular source code for the PySpark pipeline.
    -   `main.py`: The entry point for the Spark application.
    -   `pipeline.py`: Orchestrates the high-level flow of the pipeline.
    -   `readers.py`: Functions for reading raw data sources.
    -   `transformers.py`: Core data transformation and feature engineering logic.
    -   `schemas.py`: Defines the explicit schemas for all input data.
    -   `utils.py`: Utility functions, including the SparkSession builder.
-   `tests/`: Contains unit and integration tests for the project using pytest.
-   `Dockerfile`: Defines the container environment for the Spark application.
-   `docker-compose.yml`: Orchestrates the Spark cluster and the processing job.
-   `start.sh`: A simple script to launch the entire pipeline.
-   `pyproject.toml`: Project dependencies managed by Poetry.

## High-Level Design

### Training Input Structure

The final training data is structured in a "wide" format, where each row contains all the information needed for a single training example. This design minimizes I/O during model training, allowing for maximum GPU utilization.

-   **Schema**: `customer_id`, `item_id`, `is_order`, `actions` (array of 1000 item IDs), `action_types` (array of 1000 action types).
-   **Format**: The data is saved in **Parquet** format, partitioned by `dt` (date). Parquet's columnar storage is highly efficient for Spark and deep learning frameworks.

### PyTorch Training Workflow

This section describes how the generated training data is intended to be used in a PyTorch environment.

The training process will iterate through the 14 days of data, one day at a time, for multiple epochs. For each day:

1.  **Data Loading**: The corresponding Parquet partition (e.g., `output/dt=2025-08-16/`) will be loaded into memory.
2.  **PyTorch Dataset**: A custom PyTorch `Dataset` will be implemented. Its `__getitem__` method will be extremely fast, as it only needs to retrieve a single pre-processed row and convert its columns into PyTorch Tensors.
3.  **DataLoader and Sampling**: A PyTorch `DataLoader` will wrap the `Dataset`, with `shuffle=True` to handle the random sampling of all impressions within that day.
4.  **Training Loop**: The `DataLoader` will produce batches that can be directly fed into the model's `forward(self, impressions: Tensor, actions: Tensor, action_types: Tensor)` method.

This pre-computation strategy is key to performance. The expensive task of finding and padding the last 1000 actions for each customer is done once in Spark. This frees the training script's CPU from heavy lifting, preventing data loading from becoming a bottleneck and ensuring the GPU remains fully utilized.

### Data Pipeline

The PySpark pipeline consists of the following modular steps:

1.  **Read Data**: Ingestion functions in `readers.py` load the raw data using predefined, explicit schemas from `schemas.py`.
2.  **Unify Actions**: A transformer in `transformers.py` combines `clicks`, `add_to_carts`, and `previous_orders` into a single DataFrame, assigning a unique integer to each action type (`1`: click, `2`: add-to-cart, `3`: order).
3.  **Get Customer Actions**: For each customer, it efficiently finds the last 1000 actions that occurred *before* the `process_date` using a window function. Sequences are padded with zeros if they have fewer than 1000 actions.
4.  **Process Impressions**: Filters impressions for the given `process_date` and explodes the `impressions` array so that each row represents a single impression.
5.  **Join and Finalize**: The `pipeline.py` orchestrator joins the processed impressions with the customer action sequences on `customer_id` and handles any null values to create the final training DataFrame.

### Scalability and Performance

-   **Explicit Schemas**: Defining schemas upfront in `schemas.py` avoids slow and error-prone schema inference at runtime.
-   **Native Spark Functions**: The pipeline avoids Python UDFs in favor of highly optimized, native Spark functions for all transformations, including the padding of action sequences. This prevents performance bottlenecks caused by data serialization between the JVM and Python.
-   **Caching**: The unified `actions` DataFrame is cached in memory (`.cache()`) because it is used in the subsequent step to build customer action histories. This prevents Spark from having to recompute it from scratch.
-   **Partitioning**: The output data is partitioned by date (`dt`), allowing training jobs to efficiently select specific date ranges without scanning the entire dataset.
-   **Containerization**: Docker ensures the pipeline runs in a consistent and isolated environment, making it scalable and portable across different systems.

## How to Run the Project

The entire application is containerized, so you only need Docker to run it.

### Prerequisites

-   Docker
-   Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd pyspark-coding-challenge
    ```

2.  **Create the data directory and add input files:**
    ```bash
    mkdir -p data/impressions data/clicks data/add_to_carts data/previous_orders
    ```
    Place your raw data files into the corresponding subdirectories (e.g., `data/impressions/impressions.json`).

### Running the Pipeline

Simply run the startup script. It will build the Docker image, start the Spark cluster, run the job, and then shut everything down.

```bash
./start.sh
````

The output will be saved in the `data/output/` directory, partitioned by the processing date.

## Running Tests

The project includes a comprehensive test suite using `pytest`.

1.  **Build the image:**

    ```bash
    docker-compose build spark-job
    ```

2.  **Run pytest inside the container:**

    ```bash
    docker-compose run --rm spark-job poetry run pytest
    ```
