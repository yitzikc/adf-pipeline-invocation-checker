#!/usr/bin/env python3

"""
This is a script to verify the parameter passing to pipelines that
are invoked either from other pipelines or from triggers. It checks that all the parameters
that don't have default values are passed, and that those that do have default values aren't
passed values equal to their default values. To run it, pass the root directory of the ADF folder
hierarchy as its argument. If it isn't passed, the current working directory is assumed.
"""

import os
import json
import logging
import sys

def load_resources(directory, resource_type, expected_type):
    """
    Load resources (pipelines or triggers) from a specific directory.
    
    Args:
        directory (str): The directory to search for resources.
        resource_type (str): Resource type (e.g., "pipeline", "trigger").
        expected_type (str): Expected resource type in the JSON (e.g., "Microsoft.DataFactory/factories/pipelines").
    
    Returns:
        dict: A dictionary of resource names and their properties.
    """
    resources = {}
    if not os.path.exists(directory):
        logging.warning(f"{resource_type.capitalize()} directory '{directory}' does not exist.")
        return resources

    for file in os.listdir(directory):
        if file.endswith(".json"):
            path = os.path.join(directory, file)
            with open(path, "r") as f:
                try:
                    file_content = json.load(f)
                    if file_content.get("type", expected_type) == expected_type:
                        resources[file_content["name"]] = file_content["properties"]
                    else:
                        logging.warning(f"Unexpected non-{resource_type} file in '{resource_type}': {file}")
                except json.JSONDecodeError as e:
                    logging.error(f"Error parsing {file} in '{resource_type}': {e}")
                except KeyError as e:
                    logging.error(f"Malformed {resource_type} file {file}: {e}")
    return resources

def load_pipelines_and_triggers(adf_dir):
    """
    Load pipelines and triggers from their respective subdirectories.
    
    Args:
        adf_dir (str): The top-level ADF directory.
    
    Returns:
        tuple: Dictionaries of pipelines and triggers.
    """
    pipelines_dir = os.path.join(adf_dir, "pipeline")
    triggers_dir = os.path.join(adf_dir, "trigger")
    pipelines = load_resources(pipelines_dir, "pipeline", "Microsoft.DataFactory/factories/pipelines")
    triggers = load_resources(triggers_dir, "trigger", "Microsoft.DataFactory/factories/triggers")
    return pipelines, triggers

def find_missing_and_redundant_params_in_activities(parent_pipeline, child_pipelines):
    """Check ExecutePipeline activities in a parent pipeline for parameter issues."""
    issues = []
    execute_activities = [
        activity for activity in parent_pipeline.get("activities", [])
        if activity["type"] == "ExecutePipeline"
    ]
    
    for activity in execute_activities:
        child_pipeline_name = activity["typeProperties"]["pipeline"]["referenceName"]
        if child_pipeline_name not in child_pipelines:
            issues.append({
                "activity": activity["name"],
                "issue": f"Child pipeline '{child_pipeline_name}' not found."
            })
            continue

        child_pipeline = child_pipelines[child_pipeline_name]
        child_parameters = child_pipeline.get("parameters", {})

        # Parameters passed to the activity
        provided_parameters = activity.get("typeProperties", {}).get("parameters", {})
        
        # Check for missing required parameters
        missing_params = [
            param for param, details in child_parameters.items()
            if "defaultValue" not in details and param not in provided_parameters
        ]
        
        # Check for redundant parameters (those matching default values)
        redundant_params = [
            param for param, details in child_parameters.items()
            if "defaultValue" in details and param in provided_parameters and 
               str(provided_parameters[param]) == str(details["defaultValue"])
        ]

        if missing_params:
            issues.append({
                "activity": activity["name"],
                "issue": f"Missing required parameters: {missing_params}"
            })

        if redundant_params:
            issues.append({
                "activity": activity["name"],
                "issue": f"Redundant parameters matching default values: {redundant_params}"
            })
    
    return issues

def find_missing_and_redundant_params_in_triggers(triggers, pipelines):
    """Check triggers to ensure all required parameters are provided."""
    issues = []
    for trigger_name, trigger in triggers.items():
        for pipeline_reference in trigger.get("pipelines", []):
            pipeline_name = pipeline_reference["pipelineReference"]["referenceName"]
            provided_parameters = pipeline_reference.get("parameters", {})
            
            if pipeline_name not in pipelines:
                issues.append({
                    "trigger": trigger_name,
                    "issue": f"Pipeline '{pipeline_name}' not found for trigger."
                })
                continue
            
            pipeline = pipelines[pipeline_name]
            pipeline_parameters = pipeline.get("parameters", {})

            # Check for missing required parameters
            missing_params = [
                param for param, details in pipeline_parameters.items()
                if "defaultValue" not in details and param not in provided_parameters
            ]

            # Check for redundant parameters
            redundant_params = [
                param for param, details in pipeline_parameters.items()
                if "defaultValue" in details and param in provided_parameters and 
                   str(provided_parameters[param]) == str(details["defaultValue"])
            ]

            if missing_params:
                issues.append({
                    "trigger": trigger_name,
                    "issue": f"Missing required parameters for pipeline '{pipeline_name}': {missing_params}"
                })
            
            if redundant_params:
                issues.append({
                    "trigger": trigger_name,
                    "issue": f"Redundant parameters matching default values for pipeline '{pipeline_name}': {redundant_params}"
                })
    
    return issues

def main():
    # Get the top-level ADF directory from command-line argument or default to the current working directory
    adf_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

    pipelines, triggers = load_pipelines_and_triggers(adf_dir)
    all_issues = []

    # Validate pipelines
    for name, pipeline in pipelines.items():
        issues = find_missing_and_redundant_params_in_activities(pipeline, pipelines)
        if issues:
            all_issues.append({
                "type": "Pipeline",
                "name": name,
                "issues": issues
            })

    # Validate triggers
    trigger_issues = find_missing_and_redundant_params_in_triggers(triggers, pipelines)
    if trigger_issues:
        all_issues.extend({
            "type": "Trigger",
            "name": issue["trigger"],
            "issues": [issue["issue"]]
        } for issue in trigger_issues)

    # Print results
    if all_issues:
        print("Validation issues found:")
        for issue in all_issues:
            print(f"\n{issue['type']} '{issue['name']}':")
            for detail in issue["issues"]:
                print(f"  {detail}")
    else:
        print("No validation issues found.")

if __name__ == "__main__":
    main()
