# -*- coding: utf-8 -*-

import sys, os, json

input_string = "{"
input_string += "'string': 'Which rivers flow through Seoul?', "
input_string += "'language': 'en', "
input_string += "'conf': {}"
input_string += "}"

input_string = '"' + input_string.replace("'", "\\\"") + '"'
print input_string

os.system('python cm_terminal.py ' + input_string)