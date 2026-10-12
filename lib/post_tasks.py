import logging
import subprocess
import os

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

def run_post_tasks(post_tasks_config):
    """
    Executes a list of post-ingestion tasks based on their type.
    Supported types: 'dbt', 'shell', 'python'
    """
    if not post_tasks_config:
        logger.info("No post-ingestion tasks configured.")
        return True

    for task in post_tasks_config:
        task_type = task.get('type')
        logger.info(f"Starting post-ingestion task of type: {task_type}")
        
        if task_type == 'dbt':
            if not _run_dbt_task(task):
                return False
        elif task_type == 'shell':
            if not _run_shell_task(task):
                return False
        elif task_type == 'python':
            if not _run_python_task(task):
                return False
        else:
            logger.warning(f"Unknown task type: {task_type}. Skipping.")
            
    logger.info("All post-ingestion tasks completed successfully.")
    return True

def _run_dbt_task(task):
    dbt_dir = task.get('project_dir')
    profiles_dir = task.get('profiles_dir')
    
    if not dbt_dir or not profiles_dir:
        logger.error("DBT directories not configured properly in task.")
        return False
        
    logger.info(f"Running DBT in {dbt_dir}...")
    cmd_run = ["dbt", "run", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    res_run = subprocess.run(cmd_run, capture_output=True, text=True)
    
    if res_run.returncode != 0:
        logger.error(f"dbt run failed!\n{res_run.stdout}\n{res_run.stderr}")
        return False
        
    cmd_test = ["dbt", "test", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    res_test = subprocess.run(cmd_test, capture_output=True, text=True)
    
    if res_test.returncode != 0:
        logger.error(f"dbt test failed!\n{res_test.stdout}\n{res_test.stderr}")
        return False
        
    return True

def _run_shell_task(task):
    command = task.get('command')
    if not command:
        logger.error("Shell command not provided.")
        return False
    command = os.path.expandvars(command)
        
    logger.info(f"Executing shell command: {command}")
    res = subprocess.run(command, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        logger.error(f"Shell task failed (RC {res.returncode}):\n{res.stdout}\n{res.stderr}")
        return False
    logger.info(f"Shell task output:\n{res.stdout}")
    return True

def _run_python_task(task):
    script_path = task.get('script')
    if not script_path or not os.path.exists(script_path):
        logger.error(f"Python script {script_path} not found.")
        return False
        
    logger.info(f"Executing python script: {script_path}")
    res = subprocess.run(["python", script_path], capture_output=True, text=True)
    if res.returncode != 0:
        logger.error(f"Python task failed (RC {res.returncode}):\n{res.stdout}\n{res.stderr}")
        return False
    logger.info(f"Python task output:\n{res.stdout}")
    return True
