import requests
import pandas as pd
import datetime
import urllib.parse
import typing
import os

PAT = os.environ.get("DREMIO_PAT", "")
DREMIO_URL = "dev.dremio.site"


def get_projects() -> typing.List[typing.Dict[str, str]]:
    global PAT, DREMIO_URL
    response = requests.get(f'https://api.{DREMIO_URL}/v0/projects',
                            headers={'Authorization': f'Bearer {PAT}',
                                     'Content-Type': 'application/json'})
    response.raise_for_status()
    return {proj['id']: proj['name'] for proj in response.json()}


def get_engines(project_id: str) -> typing.Dict[str, typing.Dict]:
    global PAT, DREMIO_URL
    response = requests.get(f'https://api.{DREMIO_URL}/v0/projects/{project_id}/engines',
                            headers={'Authorization': f'Bearer {PAT}',
                                     'Content-Type': 'application/json'})
    response.raise_for_status()
    return {engines['id']: engines for engines in response.json()}


def build_projects_and_engines_table() -> pd.DataFrame:
    projects = get_projects()
    data = []
    for id, name in projects.items():
        for eid, engine in get_engines(id).items():
            data.append({'project_id': id, 'project_name': name, 'engine_id': eid, 'engine_name': engine['name'],
                         'engine_size': engine['size'], 'instanceFamily': engine['instanceFamily']})
    return pd.DataFrame.from_records(data)


def parse_dates(usage: typing.Dict) -> typing.Dict:
    for key in ('startTime', 'endTime'):
        usage[key] = datetime.datetime.fromisoformat(usage[key])
    return usage


def get_usage_per_project(last_n_days=30) -> typing.List[typing.Dict]:
    global PAT, DREMIO_URL
    start_time = int((datetime.datetime.now() - datetime.timedelta(days=last_n_days)).timestamp() * 1000)
    response = requests.get(f'https://api.{DREMIO_URL}/v0/usage',
                            headers={'Authorization': f'Bearer {PAT}',
                                     'Content-Type': 'application/json'},
                            params={'frequency': 'DAILY', 'maxResults': 500,
                                    'filter': f'start_time >= {start_time}'})
    response.raise_for_status()

    # TODO: handle pagination
    return [parse_dates(usage) for usage in response.json()['data']]


def get_usage_per_engine(usage: typing.Dict) -> typing.List[typing.Dict]:
    global PAT, DREMIO_URL
    start_time = int(usage['startTime'].timestamp() * 1000)
    end_time = int(usage['endTime'].timestamp() * 1000)
    project_id = usage['id']
    params = {'frequency': 'DAILY', 'groupBy': 'ENGINE',
                                    'filter': f"project_id+==+'{project_id}' && start_time >= {start_time}"\
                                              f" && start_time <= {end_time}"}
    response = requests.get(f'https://api.{DREMIO_URL}/v0/usage',
                            headers={'Authorization': f'Bearer {PAT}',
                                     'Content-Type': 'application/json'},
                            params=urllib.parse.urlencode(params, safe="+"))
    response.raise_for_status()

    # TODO: handle pagination
    return [parse_dates(usage) for usage in response.json()['data']]


def build_usage_per_engine_table(last_n_days=30) -> pd.DataFrame:
    project_usages = get_usage_per_project(last_n_days)
    data = []
    for usage in [usage for usage in project_usages if usage['usage'] > 0]:
        for eu in get_usage_per_engine(usage):
            data.append({'project_id': usage['id'], 'start_time': usage['startTime'], 'end_time': usage['endTime'],
                         'project_usage': usage['usage'],
                         'engine_id': eu['id'], 'engine_start': eu['startTime'], 'end_time': eu['endTime'],
                         'engine_usage': eu['usage']})
    return pd.DataFrame.from_records(data)
