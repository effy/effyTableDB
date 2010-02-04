#!/usr/bin/env python
#
# effyTable client.
#
# Copyright 2010 Toshio Koide.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from effyTable import effyTable
from multiprocessing.managers import BaseManager
import sys

if len(sys.argv) < 3:
    print "usage: %s <address> <port> <authkey>" % sys.argv[0]
    exit(1)

[address, port, authkey] = sys.argv[1:]

BaseManager.register('gettable', callable=lambda:t)
m = BaseManager(address=(address, int(port)), authkey=authkey)
m.connect()

t = m.gettable()

t.setIndex('age')
t.setIndex('name')

from random import random
for i in xrange(10000):
    t.addRow({'name':'hoge', 'age':random()})

