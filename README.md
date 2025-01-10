# ADF Pipeline and Trigger Validation Script

This Python script validates Azure Data Factory (ADF) pipeline and trigger configurations stored in a Git repository. It checks that all required parameters are provided to `ExecutePipeline` activities and triggers, and identifies redundant parameters that are passed which repeat their default values.

## Features
- **Pipeline Validation**: Ensures that `ExecutePipeline` activities in parent pipelines provide all required parameters and that no redundant parameters (matching default values) are passed.
- **Trigger Validation**: Verifies that triggers pass all required parameters to the invoked pipelines and flags redundant parameters.
- **Custom Directory Support**: Loads pipelines from the `pipeline` directory and triggers from the `trigger` directory.
- **Customizable Root Directory**: Accepts the root directory of the ADF project as an argument (defaults to the current working directory).
- **Error Handling**: Logs warnings and errors for missing or malformed files and issues in the validation process.

## Requirements
- Python 3.x
- `os`, `json`, `logging`, and `sys` libraries (standard in Python)

## Usage
1. Clone the repository containing the ADF pipeline and trigger JSON files.
2. Ensure the ADF project directory contains `pipeline` and `trigger` subdirectories.
3. Run the script with the following command:

```bash
python validate_adf.py /path/to/adf/factory
```

- Replace `/path/to/adf/factory` with the path to the ADF project directory. If not provided, the script will use the current working directory by default.

## Output
The script outputs validation results to the console:
- Lists any missing or redundant parameters for each pipeline and trigger.
- Flags pipelines or triggers that are missing or incorrectly configured.

## Example Output:
```bash
Validation issues found:

Pipeline 'MainPipeline':
  Activity 'ExecutePipeline1': Missing required parameters: ['Param1', 'Param2']
  Activity 'ExecutePipeline2': Redundant parameters matching default values: ['Param3']

Trigger 'DailyTrigger':
  Redundant parameters matching default values for pipeline 'MainPipeline': ['Param4']
```

## Customization
- The script can be modified to handle additional resource types, add more validation checks, or change the directory structure as needed.

## License
This script is licensed under the MIT License.
