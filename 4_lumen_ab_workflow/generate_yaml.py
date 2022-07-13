import os
import yaml

os.chdir('')
print(os.listdir())

class literal_str(str):
    pass

def change_style(style, representer):
    def new_representer(dumper, data):
        scalar = representer(dumper, data)
        scalar.style = style
        return scalar
    return new_representer
from yaml.representer import SafeRepresenter
represent_literal_str = change_style('|', SafeRepresenter.represent_str)
yaml.add_representer(literal_str, represent_literal_str)

class MyDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)

workflow = {'version': '2.0', 'workflow': {'tasks': {}}}

for index, file in enumerate(os.listdir()):
    if file == '03_identify_new_ab_fields.py':
        filename = file.split('.')[0]
        run_app_str = '''python /app/''' + filename + '.py'
        task_dict = {filename: {'action': 'civis.container_script', 'input': {'name': filename,
                    'docker_image_name': 'civisanalytics/datascience-python',
                    'required_resources': {'cpu': 1024, 'memory': 1024, 'disk_space': 1.0},
                    'repo_http_uri': 'github.com/CWA-union/lumen-ab-workflow.git',
                    'repo_ref': 'test',
                    'docker_command': literal_str(run_app_str),
                    'notifications': {'successOn': True, 'successEmailAddresses': ['tzhu@cwa-union.org'], 
                    'successEmailSubject': literal_str('New AB Fields to Add'), 'successEmailBody': literal_str('''CWA Locals: {{cwa_locals}}\nWork Locations: {{work_locations}}'''), 'failureEmailAddresses': ['tzhu@cwa-union.org'], 
                    'failureOn': True}}, 'on-success': ['04_workloc_inactive']}}
        workflow['workflow']['tasks'].update(task_dict)
    elif file == '15_email_job_success.txt':
        filename = file.split('.')[0]
        text_file = open(file, "r")
        data = text_file.read()
        new_data = literal_str(data)
        text_file.close()
        task_dict = {filename: {'action': 'civis.scripts.python3', 'input': {'notifications': 
            {'successOn': True, 'successEmailAddresses': ['tzhu@cwa-union.org'], 
            'successEmailSubject': literal_str('Lumen ActionBuilder Dues/Barg Process Final Workflow Summary'), 
            'successEmailBody': new_data, 'failureEmailAddresses': ['tzhu@cwa-union.org'], 
            'failureOn': True}, 'source': literal_str('var = 100')}}}
        workflow['workflow']['tasks'].update(task_dict)
    elif '.sql' in file:
        filename = file.split('.')[0]
        text_file = open(file, "r")
        data = text_file.read()
        new_data = literal_str(data)
        text_file.close()
        next_index = index + 1
        try:
            next_file = os.listdir()[next_index]
            next_filename = next_file.split('.')[0]
        except IndexError:
            print('End of the line.')
            next_file = None
        task_dict = {filename: {'action': 'civis.scripts.sql', 'input': {'sql': new_data,
                    'remote_host_id': 1892, 'credential_id': 19284}}}
        if next_file:
            task_dict[filename].update({'on-success': [next_filename]})
        workflow['workflow']['tasks'].update(task_dict)
    elif '.py' in file and 'get_run_outputs' not in file and 'generate' not in file:
        filename = file.split('.')[0]
        next_index = index + 1
        try:
            next_file = os.listdir()[next_index]
            next_filename = next_file.split('.')[0]
        except IndexError:
            print('End of the line.')
            next_file = None
        run_app_str = '''python /app/''' + filename + '.py'
        task_dict = {filename: {'action': 'civis.container_script', 'input': {'name': filename,
                    'docker_image_name': 'civisanalytics/datascience-python',
                    'required_resources': {'cpu': 1024, 'memory': 1024, 'disk_space': 1.0},
                    'repo_http_uri': 'github.com/CWA-union/lumen-ab-workflow.git',
                    'repo_ref': 'test',
                    'docker_command': literal_str(run_app_str)}}}
        if next_file and 'generate' not in next_file:
            task_dict[filename].update({'on-success': [next_filename]})
        workflow['workflow']['tasks'].update(task_dict)

with open("workflow.yaml", 'a+') as f:
    f.write(yaml.dump(workflow, Dumper=MyDumper))