import civis
import os

client = civis.APIClient()

workflow_id = 65841
executions = client.workflows.list_executions(id=workflow_id)
execution_id = executions[0]['id']
execution = client.workflows.get_executions(id=workflow_id, execution_id=execution_id)

execution_tasks = execution['tasks']

task_list = []
for i in execution_tasks:
    print(i['name'])
    task_list.append(i['name'])

task_list.reverse()

for task_name in task_list:
    task = client.workflows.get_executions_tasks(id=workflow_id, execution_id=execution_id, task_name=task_name)
    job_id = task['runs'][0]['job_id']
    run_id = task['runs'][0]['id']
    outputs = client.jobs.list_runs_outputs(id=job_id, run_id=run_id)
    try:
        print(task_name)
        for i in outputs[0]['value']:
            print(i + ': ' + str(outputs[0]['value'][i]))
        print('\n')
    except:
        pass