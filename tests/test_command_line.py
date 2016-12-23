#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import subprocess

import webtest


class TestCommandLine(object):

    def setup(self):
        """Set up the environment by moving to the demos directory."""
        os.chdir("demos")

    def teardown(self):
        os.chdir("..")

    def add(self, *args):
        self.db.add_all(args)
        self.db.commit()

    def test_dallinger_help(self):
        output = subprocess.check_output("dallinger", shell=True)
        assert("Usage: dallinger [OPTIONS] COMMAND [ARGS]" in output)


class TestDebugCommand(object):

    def setup(self):
        """Set up the environment by moving to the demos directory."""
        os.chdir("demos/snake")

    def teardown(self):
        os.chdir("../..")

    def test_debug_launches_gunicorn_application(self):
        from dallinger.experiment_server.gunicorn import StandaloneServer
        from dallinger.command_line import debug
        original_run = StandaloneServer.run
        nonlocal_data = {}
        def run(self):
            nonlocal_data['wsgi'] = self.wsgi()
        try:
            StandaloneServer.do_load_config = lambda x:None
            StandaloneServer.run = run
            debug.callback(True)
        finally:
            StandaloneServer.run = original_run
        app = webtest.TestApp(nonlocal_data['wsgi'])
        response = app.get('/ad')
        assert response.status_code == 200
        assert 'consent' in response
