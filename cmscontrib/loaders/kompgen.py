#!/usr/bin/env python3

# Programming contest management system
# Copyright © 2020 Kevin Atienza <kevin.charles.atienza@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import os
import stat

from cms.db import (
    Contest, User, Task, Statement, Attachment, Dataset, Manager, Testcase,
)
from cmscommon.constants import (
    SCORE_MODE_MAX,
    SCORE_MODE_MAX_SUBTASK,
    SCORE_MODE_MAX_TOKENED_LAST,
)
from cmscommon.crypto import build_password
from .base_loader import ContestLoader, TaskLoader, UserLoader


logger = logging.getLogger(__name__)


KG_TASK = 'kg_cms_task.json'
KG_CONTEST = 'kg_cms_contest.json'
KG_USERS = 'kg_cms_users.json'

class KGLoaderException(Exception): ...


class IOPair:
    def __init__(self, input=None, output=None):
        self.input = input
        self.output = output
        super().__init__()

# these defaults may not necessarily match CMS defaults

DATASET_DEFAULTS = {
    'task_type': 'Batch',
    'score_type': 'Sum',
    'score_type_parameters': 100,
    'autojudge': True,
    'time_limit': 3,
    'memory_limit': 512 << 20, # 512 Mb
}

TASK_DEFAULTS = {
    'name': None,
    'title': None,
    'max_submission_number': 200,
    'max_user_test_number': 30,
    'min_submission_interval': 60,
    'min_user_test_interval': 60,
    'score_precision': 0,
    'score_mode': SCORE_MODE_MAX,
}

LANGUAGE_MAPPING = {
    'c++': 'C++17 / g++',
    'cpp': 'C++17 / g++',
    'java': 'Java / JDK',
    'python3': 'Python 3 / CPython',
    'pypy3': 'Python 3 / PyPy',
}

CONTEST_DEFAULTS = {
    'name': None,
    'description': None,
    'languages': ['cpp', 'java', 'python3'],
    'submissions_download_allowed': True,
    'allow_questions': True,
    'allow_user_tests': True,
    'block_hidden_participations': False,
    'allow_password_authentication': True,
    'start': None,
    'stop': None,
    'timezone': None,
    'per_user_time': None,
    'max_submission_number': 500,
    'max_user_test_number': 100,
    'min_submission_interval': 1,
    'min_user_test_interval': 1,
    'score_precision': 0,
}

def make_executable(filename):
    os.chmod(
        filename,
        os.stat(filename).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class KGTaskLoader(TaskLoader):
    """Load a task stored using the KompGen cms-compiled format.

    Given the filesystem location of a task prepared via KompGen and compiled
    via `kg make all` and then `kg kompile cms` (by default located at
    `kgkompiled/cms`), parse the contents to produce a CMS Task object.

    Not all options are supported yet.
    """

    short_name = 'kg_task'
    description = 'KompGen CMS task format'

    @staticmethod
    def detect(path):
        return os.path.exists(os.path.join(path, KG_TASK))

    def task_has_changed(self):
        return True

    def _get_task_config(self):
        kg_task = os.path.join(self.path, KG_TASK)
        logger.info(f"Reading {kg_task}")
        with open(kg_task) as file:
            return json.load(file)

    def _get_task(self, task_config, get_statement=True):

        # initialize the basic fields
        logger.info("Creating the Task object")
        fields = {
            field: task_config.get(field, default)
            for field, default in TASK_DEFAULTS.items()
        }

        name = fields['name']
        if not name:
            raise KGLoaderException("Invalid/Missing name")

        # load the statement
        if get_statement:
            logger.info("Loading the statement")
            fields['statements'] = {}
            lang = 'en'
            statement_path = os.path.join(self.path, task_config['statement'])
            if not os.path.isfile(statement_path):
                raise KGLoaderException(
                    f"Missing statement, expected in {statement_path}")
            digest = self.file_cacher.put_file_from_path(
                statement_path, f"Statement for Task: {name}")
            fields['primary_statements'] = [lang]
            fields['statements'][lang] = Statement(lang, digest)

        # load the attachments
        # TODO Python 3.8
        attachments = task_config.get('attachments', [])
        if attachments:
            logger.info("Loading the attachments")
            fields['attachments'] = {}
            for attachment in attachments:
                path = os.path.join(self.path, 'attachments', attachment)
                if not os.path.isfile(path):
                    raise KGLoaderException(
                        f"Missing attachment, expected in {path}")
                if attachment in fields['attachments']:
                    raise KGLoaderException(
                        f"Duplicate attachment: {attachment}")
                digest = self.file_cacher.put_file_from_path(
                    path, f"Attachment for Task: {name}")
                fields['attachments'][attachment] = Attachment(
                    attachment, digest)

        # set task-type-specific fields
        task_type = task_config['task_type']
        logger.info(f"The task type is {task_type}")
        if task_type in {'Batch', 'Communication'}:
            fields['submission_format'] = [f'{name}.%l']
        else:
            raise KGLoaderException(f"Unsupported task type: {task_type}")

        # convert some fields to their required types
        for field in 'min_submission_interval', 'min_user_test_interval':
            if isinstance(fields[field], (int, float)):
                fields[field] = timedelta(seconds=fields[field])

        return Task(**fields)

    def _create_and_attach_dataset(self, task_config, task):
        # create dataset
        logger.info("Creating the Dataset")
        fields = {
            field: task_config.get(field, default)
            for field, default in DATASET_DEFAULTS.items()
        }
        fields['task'] = task
        fields['description'] = "Default"
        fields['managers'] = {}

        # set checker
        checker_path = os.path.join(self.path, 'checker')
        if os.path.exists(checker_path):
            logging.info("Loading the checker")
            make_executable(checker_path) # force it to be executable
            digest = self.file_cacher.put_file_from_path(
                checker_path, f"Checker for Task: {task.name}")
            fields['managers']['checker'] = Manager('checker', digest)
            evaluation_param = 'comparator'
        else:
            logger.warn("Checker not found, using diff")
            evaluation_param = 'diff'

        # set manager ("interactor")
        manager_path = os.path.join(self.path, 'manager')
        if os.path.exists(manager_path):
            logging.info("Loading the manager")
            make_executable(manager_path) # force it to be executable
            digest = self.file_cacher.put_file_from_path(
                manager_path, f"Manager for Task: {task.name}")
            fields['managers']['manager'] = Manager('manager', digest)

        # 'alone' means there's no accompanying grader.cpp
        if fields['task_type'] == 'Batch':
            fields['task_type_parameters'] = [
                'alone', ["", ""], evaluation_param,
            ]

        if fields['task_type'] == 'Communication':
            fields['task_type_parameters'] = [
                task_config['node_count'], 'alone', task_config['io_type'],
            ]

        # read test data from tests/
        logger.info("Reading the test data")
        test_bases = defaultdict(IOPair)
        tests_path = os.path.join(self.path, 'tests')
        for test_filename in os.listdir(tests_path):
            test_base, input_ext = os.path.splitext(
                os.path.basename(test_filename))
            test_base = test_bases[test_base]
            if input_ext == '.in':
                test_base.input = test_filename
            elif input_ext == '.ans':
                test_base.output = test_filename
            else:
                raise KGLoaderException(
                    f"Unrecognized file found in tests/: {test_filename}")

        # check for missing I/O
        # TODO Python 3.8
        bad_io = [io_base
            for io_base, io_pair in test_bases.items()
            if not (io_pair.input and io_pair.output)
        ]
        if bad_io:
            raise KGLoaderException(
                f"These tests have missing input or output: {bad_io}")

        if not test_bases:
            raise KGLoaderException("tests/ must not be empty")

        # load test cases
        logger.info(f"Found {len(test_bases)} cases. Loading the test data")
        fields['testcases'] = {}
        for test_basename, test_base in sorted(test_bases.items()):
            fields['testcases'][test_basename] = Testcase(
                    test_basename, True,
                    self.file_cacher.put_file_from_path(os.path.join(
                        tests_path, test_base.input),
                        f"Input {test_basename} for Task: {task.name}"),
                    self.file_cacher.put_file_from_path(os.path.join(
                        tests_path, test_base.output),
                        f"Output {test_basename} for Task: {task.name}"),
                )

        # convert some fields to their required types
        for field in 'time_limit',:
            fields[field] = float(fields[field])

        dataset = Dataset(**fields)

        # set it as the active dataset
        task.active_dataset = dataset

        return dataset

    def get_task(self, get_statement=True):
        # load the task config file
        task_config = self._get_task_config()

        # construct the task object
        task = self._get_task(task_config, get_statement=get_statement)

        # attach the dataset
        self._create_and_attach_dataset(task_config, task)

        logger.info(f"Task {task.name!r} successfully loaded.")
        return task


def contest_path_candidates(path):
    yield path
    yield os.path.dirname(path) # needed when importing individual users


class KGContestLoader(ContestLoader, UserLoader):
    """Load a contest and users stored using the KompGen cms-compiled format.

    Given the filesystem location of a contest prepared via KompGen and compiled
    via `kg contest cms`, parse the contents to produce a CMS Task object.

    Not all options are supported yet.
    """

    short_name = 'kg_contest'
    description = 'KompGen CMS contest format'

    @staticmethod
    def detect(path):
        return any(os.path.exists(os.path.join(directory, KG_CONTEST))
            for directory in contest_path_candidates(path))

    def user_has_changed(self):
        return True

    def contest_has_changed(self):
        return True

    def get_task_loader(self, taskname):
        return KGTaskLoader(os.path.join(self.path, taskname), self.file_cacher)

    def _get_contest_config(self):
        for directory in contest_path_candidates(self.path):
            kg_contest = os.path.join(directory, KG_CONTEST)
            if os.path.exists(kg_contest):
                logger.info(f"Reading {kg_contest}")
                with open(kg_contest) as file:
                    return json.load(file)
        else:
            raise KGLoaderException(
                f"Couldn't find contest config file {KG_CONTEST}")

    def _get_user_list(self):
        for directory in contest_path_candidates(self.path):
            kg_users = os.path.join(directory, KG_USERS)
            if os.path.exists(kg_users):
                logging.info(f"Reading {kg_users}")
                with open(kg_users) as file:
                    return json.load(file)
        else:
            raise KGLoaderException(
                f"Couldn't find the user list {KG_USERS}")

    def _get_minimal_participations(self):
        participations = []
        for user in self._get_user_list():
            if user['type'] == 'user':
                participations.append({
                        'username': user['username'],
                        'password': build_password(user['password']),
                    })

        return participations

    def _get_contest(self, contest_config):

        # initialize the basic fields
        logger.info("Creating the Contest object")
        fields = {
            field: contest_config.get(field, default)
            for field, default in CONTEST_DEFAULTS.items()
        }

        # convert lang names
        fields['languages'] = [
            LANGUAGE_MAPPING.get(lang, lang)
            for lang in fields['languages']
        ]

        # convert datetimes
        for field in 'start', 'stop':
            if isinstance(fields[field], (int, float)):
                fields[field] = datetime.utcfromtimestamp(
                    fields[field])

        # convert timedeltas
        for field in 'min_submission_interval', 'min_user_test_interval':
            if isinstance(fields[field], (int, float)):
                fields[field] = timedelta(seconds=fields[field])

        return Contest(**fields)

    def get_user(self):
        username = os.path.basename(self.path)
        for user in self._get_user_list():
            if user['type'] == 'user' and user['username'] == username:
                return User(
                        username=user['username'],
                        password=build_password(user['password']),
                        first_name=user.get('first_name') or '',
                        last_name=user.get('last_name') or '',
                        timezone=user.get('timezone'),
                    )
        else:
            raise KGLoaderException(f"Unknown user: {username}")

    def get_contest(self):
        # load the contest config data
        contest_config = self._get_contest_config()

        # construct the contest object
        contest = self._get_contest(contest_config)

        # get task list
        tasks = contest_config['problems']

        # get list of participations (usernames)
        participations = self._get_minimal_participations()

        logger.info(f"Contest {contest.name!r} successfully loaded.")
        return contest, tasks, participations
