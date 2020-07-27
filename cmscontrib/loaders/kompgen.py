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
import json
import logging
import os
import stat
from datetime import timedelta#, datetime

# from cms import config
from cms.db import Contest, User, Task, Statement, Attachment, Dataset, Manager, Testcase
from cmscommon.constants import SCORE_MODE_MAX, SCORE_MODE_MAX_SUBTASK, SCORE_MODE_MAX_TOKENED_LAST
# from cmscommon.crypto import build_password
# from cmscontrib import touch
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
    'score_mode': SCORE_MODE_MAX_SUBTASK,
}

def make_executable(filename):
    os.chmod(filename, os.stat(filename).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class KGTaskLoader(TaskLoader):
    """
    Load a task stored using the KompGen cms-compiled format.

    Given the filesystem location of a task prepared via KompGen and
    compiled via `kg make allkg kompile cms` (by default located at
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
        logger.info(f"Reading {kg_task}...")
        with open(kg_task) as file:
            return json.load(file)

    def _get_task(self, task_config, get_statement=True):

        logger.info("Creating the Task object...")

        task_fields = {field: task_config.get(field, default) for field, default in TASK_DEFAULTS.items()}

        name = task_fields['name']
        if not name: raise KGLoaderException("Invalid/Missing name")

        # set statement
        if get_statement:
            task_fields['statements'] = {}
            lang = 'en'
            statement_path = os.path.join(self.path, task_config['statement'])
            if not os.path.isfile(statement_path):
                raise KGLoaderException(f"Missing statement, expected in {statement_path}")
            digest = self.file_cacher.put_file_from_path(statement_path, f"Statement for Task: {name}")
            task_fields['primary_statements'] = [lang]
            task_fields['statements'][lang] = Statement(lang, digest)

        # set attachments
        # TODO Python 3.8
        attachments = task_config.get('attachments', [])
        if attachments:
            task_fields['attachments'] ={}
            for attachment in attachments:
                path = os.path.join(self.path, 'attachments', attachment)
                if not os.path.isfile(path):
                    raise KGLoaderException(f"Missing attachment, expected in {path}")
                if attachment in task_fields['attachments']:
                    raise KGLoaderException(f"Duplicate attachment: {attachment}")
                digest = self.file_cacher.put_file_from_path(path, f"Attachment for Task: {name}")
                task_fields['attachments'][attachment] = Attachment(attachment, digest)

        # set task-type-specific fields
        task_type = task_config['task_type']
        if task_type == 'Batch':
            task_fields['submission_format'] = [f'{name}.%l']
        else:
            raise KGLoaderException(f"Unsupported task type: {task_type}")

        # convert some fields to their required types
        for field in 'min_submission_interval', 'min_user_test_interval':
            if isinstance(task_fields[field], int):
                task_fields[field] = timedelta(seconds=task_fields[field])

        return Task(**task_fields)

    def _create_and_attach_dataset(self, task_config, task):
        # create dataset
        dataset_fields = {field: task_config.get(field, default) for field, default in DATASET_DEFAULTS.items()}
        dataset_fields['task'] = task
        dataset_fields['description'] = "Default"
        dataset_fields['managers'] = {}

        # set checker
        checker_path = os.path.join(self.path, 'checker')
        if os.path.exists(checker_path):
            make_executable(checker_path) # force it to be executable
            digest = self.file_cacher.put_file_from_path(checker_path, f"Checker for Task: {task.name}")
            dataset_fields['managers']['checker'] = Manager('checker', digest)
            evaluation_param = 'comparator'
        else:
            logger.warn("Checker not found, using diff")
            evaluation_param = 'diff'

        dataset_fields['task_type_parameters'] = ['alone', ["", ""], evaluation_param]

        # read test data from tests/
        test_bases = defaultdict(IOPair)
        tests_path = os.path.join(self.path, 'tests')
        for test_filename in os.listdir(tests_path):
            test_base, input_ext = os.path.splitext(os.path.basename(test_filename))
            test_base = test_bases[test_base]
            if input_ext == '.in':
                test_base.input = test_filename
            elif input_ext == '.ans':
                test_base.output = test_filename
            else:
                raise KGLoaderException(f"Unrecognize file found in tests/: {test_filename}")

        # TODO Python 3.8
        bad_io = [io_base for io_base, io_pair in test_bases.items() if not (io_pair.input and io_pair.output)]
        if bad_io:
            raise KGLoaderException(f"These tests have missing input or output: {bad_io}")

        if not test_bases:
            raise KGLoaderException("tests/ must not be empty")

        dataset_fields['testcases'] = {}
        for test_basename, test_base in sorted(test_bases.items()):
            dataset_fields['testcases'][test_basename] = Testcase(
                    test_basename, True,
                    self.file_cacher.put_file_from_path(os.path.join(tests_path, test_base.input),
                        f"Input {test_basename} for Task: {task.name}"),
                    self.file_cacher.put_file_from_path(os.path.join(tests_path, test_base.output),
                        f"Output {test_basename} for Task: {task.name}"),
                )

        # convert some fields to their required types
        for field in 'time_limit',:
            dataset_fields[field] = float(dataset_fields[field])

        dataset = Dataset(**dataset_fields)
        task.active_dataset = dataset
        return dataset

    def get_task(self, get_statement=True):
        task_config = self._get_task_config()
        task = self._get_task(task_config, get_statement=get_statement)
        self._create_and_attach_dataset(task_config, task)
        logger.info(f"Task {task.name!r} successfully loaded.")
        return task
