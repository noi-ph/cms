#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
# Copyright © 2010-2013 Giovanni Mascellani <mascellani@poisson.phc.unipi.it>
# Copyright © 2010-2018 Stefano Maggiolo <s.maggiolo@gmail.com>
# Copyright © 2010-2012 Matteo Boscariol <boscarim@hotmail.com>
# Copyright © 2012-2018 Luca Wehrstedt <luca.wehrstedt@gmail.com>
# Copyright © 2014 Artem Iglikov <artem.iglikov@gmail.com>
# Copyright © 2014 Fabian Gundlach <320pointsguy@gmail.com>
# Copyright © 2016 Myungwoo Chun <mc.tamaki@gmail.com>
# Copyright © 2023-2023 Kevin Atienza <kevin@noi.ph>
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

"""Statement-related handlers for AWS for a specific contest.

"""

try:
    import tornado4.web as tornado_web
except ImportError:
    import tornado.web as tornado_web

from cms.db import Contest, Session, Statement, Task
from cmscommon.datetime import make_datetime
from .base import BaseHandler, require_permission


class ContestStatementHandler(BaseHandler):
    """Add a global statement to a contest.

    """
    @require_permission(BaseHandler.PERMISSION_ALL)
    def get(self, contest_id):
        self.contest = self.safe_get_item(Contest, contest_id)
        self.r_params = self.render_params()
        self.render("add_global_statement.html", **self.r_params)

    @require_permission(BaseHandler.PERMISSION_ALL)
    def post(self, contest_id):
        fallback_page = self.url("contest", contest_id, "globalstatement")

        contest = self.safe_get_item(Contest, contest_id)

        language = self.get_argument("language", "")
        if len(language) == 0:
            self.service.add_notification(
                make_datetime(),
                "No language code specified",
                "The language code can be any string.")
            self.redirect(fallback_page)
            return
        statement = self.request.files["statement"][0]
        if not statement["filename"].endswith(".pdf"):
            self.service.add_notification(
                make_datetime(),
                "Invalid contest statement",
                "The contest statement must be a .pdf file.")
            self.redirect(fallback_page)
            return

        contest_name = contest.name
        self.sql_session.close()

        try:
            digest = self.service.file_cacher.put_file_content(
                statement["body"],
                "Global statement for contest %s (lang: %s)" % (
                    contest_name,
                    language))
        except Exception as error:
            self.service.add_notification(
                make_datetime(),
                "Contest global statement storage failed",
                repr(error))
            self.redirect(fallback_page)
            return

        self.sql_session = Session()

        contest = self.safe_get_item(Contest, contest_id)
        for task in contest.tasks:
            self.sql_session\
                    .query(Statement)\
                    .filter(Statement.task_id == task.id)\
                    .delete()
            statement = Statement(language, digest, task=task)
            self.sql_session.add(statement)

        if self.try_commit():
            self.redirect(self.url("contest", contest_id))
        else:
            self.redirect(fallback_page)
