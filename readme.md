# DynamoDB Throughput Optimization Tool

This tool analyzes the usage patterns of DynamoDB tables and provides recommendations on how to optimize capacity provisioning. The recommendations are based on historical usage patterns, and can help reduce the cost of running DynamoDB.

### Features

- Retrieves DynamoDB table information and autoscaling settings
- Fetch CloudWatch metrics for DynamoDB tables and simulate Provisioned Capacity usage and autoscaling based on specified utilization targets and minimum read/write units
- Provides cost optimization recommendations based on specified utilization targets and minimum read/write units
- Generates summary CSV files and visualizations of current and recommended costs by the recommended mode

**Disclaimer:** This tool makes recommendations based on the number of days specified in the configuration file, which can be up to the last 14 days (configurable when running the tool). Additionally, when simulating the autoscaling behavior, it cannot consider network delays between scaling events until they are applied in reality. Therefore, it is recommended that recommendations be validated as some of them may not be applicable.

Be aware that changing the throughput mode can have an impact on the applications, so be sure to test any changes thoroughly before making them in production.

## Choosing Throughput Mode in DynamoDB

DynamoDB throughput modes determine how your tables will scale to handle varying levels of read and write traffic.It's important to understand the cost implications of each mode to make informed decisions about which mode to use.

There are two throughput modes available in DynamoDB:

- **Provisioned Mode:** In this mode, you specify the read and write capacity for your table and DynamoDB reserves that capacity for your use. You pay a predictable hourly rate for the amount of capacity you provision.

- **On-Demand Mode:** In this mode, DynamoDB automatically scales the read and write capacity for your table in response to traffic. You pay for only the requests that you make.

### Provisioned Throughput

- Provisioned throughput is ideal for workloads that have consistent traffic patterns or predictable spikes in traffic.
- Provisioned throughput provides better performance predictability and enables users to fine-tune capacity to their needs.
- Provisioned throughput is also less expensive than on-demand throughput for predictable workloads.

 **Note:** Use auto-scaling to ensure that your tables can handle capacity changes automatically without manual intervention.

### On-demand Throughput

- On-demand throughput is ideal for workloads with unpredictable or infrequent traffic patterns.
- On-demand throughput allows users to pay only for the capacity they consume without having to manually adjust capacity.
- On-demand throughput can be more expensive than provisioned throughput for predictable workloads.

**Note:** It's important to monitor your DynamoDB usage and make adjustments to the throughput mode as necessary to optimize cost and performance.

## Requirements

- Python 3.8
- AWS CLI configured with appropriate credentials and region
- Required Python packages listed in `requirements.txt

## Installation

1. Clone the repository
2.  Create Python Virtual Environment for clean install

    ```sh
    python3.8 -m venv .venv
    source .venv/bin/activate
    ```
3. Install the required Python packages:

    ```sh
    pip3 install -r requirements.txt
    ```

## Usage

1. Set up the AWS CLI with appropriate credentials and region if you haven't done so already.

2. Run the script with the desired options:


    ```sh
    python3 recommendation.py --dynamodb-tablename TABLE_NAME --dynamodb-read-utilization READ_UTILIZATION --dynamodb-write-utilization WRITE_UTILIZATION --dynamodb-minimum-write-unit MINIMUM_WRITE_UNIT --dynamodb-minimum-read-unit MINIMUM_READ_UNIT --number-of-days-look-back DAYS_LOOK_BACK [--debug]
    ```

    Replace the options with the desired values:

    - `TABLE_NAME`: DynamoDB table name (optional; if not provided, the script will process all tables in the specified region)
    - `READ_UTILIZATION`: DynamoDB read utilization (default: `70`)
    - `WRITE_UTILIZATION`: DynamoDB write utilization (default: `70`)
    - `MINIMUM_WRITE_UNIT`: DynamoDB minimum write unit (default: `1`)
    - `MINIMUM_READ_UNIT`: DynamoDB minimum read unit (default: `1`)
    - `DAYS_LOOK_BACK`: Number of days to look back (default: `14`)

    Add the `--debug` flag to save metrics and estimates as CSV files in the `output` folder.
3. The output files will be saved in the `output` folder.

## Output

Check the generated files in the `output` folder for the summary CSV file and the visualization of the recommendations.

The **`summary.csv`** file contains the following columns:

- `index_name`: The name of the index associated with the recommendation.
- `base_table_name`: The name of the base table associated with the recommendation.
- `metric_name`: The name of the metric associated with the recommendation.
- `est_provisioned_cost`: The estimated cost of the Provisioned Throughput mode based on the table's estimated usage.
- `current_provisioned_cost`: The cost of the table's current Provisioned capacity.
- `ondemand_cost`: The cost of using on-demand capacity mode for the table.
- `recommended_mode`: The recommended capacity mode for the table.
- `current_mode`: The table's current capacity mode.
- `status`: The status of the recommendation if `Optimized` or `Not Optimized`.
- `savings_pct`: The estimated percentage of cost savings by using the recommended capacity mode.
- `number_of_days`: The number of days in the lookback period for the analysis.
- `current_min_capacity`: The table's current minimum capacity.
- `simulated_min_capacity`: The minimum capacity based on what the recommended capacity mode is simulated.
- `current_target_utilization`: The table's current target utilization.
- `simulated_target_utilization`: The target utilization based on what the recommended capacity mode is simulated.
- `current_cost`: The table's current cost for period analyzed
- `recommended_cost`: The table's estimated cost for period analyzed after applying the recommendation.
- `autoscaling_enabled`: The Table's current Autoscaling Status.

## License

This project is licensed under the terms of the MIT License. See the [LICENSE](LICENSE) file for details.
