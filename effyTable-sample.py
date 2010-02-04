#!/usr/bin/env python
#
# usage example for effyTable module.
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

t = effyTable()

t.setIndex('age')
t.setIndex('name')

t.addRow({'name':'hoge', 'age':60})
t.addRow({'name':'foo', 'age':20})
t.addRow({'name':'bar', 'age':40})
t.addRow({'name':'buzz', 'age':30, 'memo':'buzzbuzzbuzz'})
t.addRow({'name':'fuzz', 'age':99, 'memo':'fuzzfuzzfuzz'})

print "=== select * from t order by name"
for id in t.getRowIds('name'):
    print t.getRow(id)

print '=== update t set name = "fizz" where name = "fuzz"'
id = t.getRowIds('name', 'fuzz')[0]
row = t.getRow(id)
row['name'] = 'fizz'
t.updateRow(id, row)

print "=== select * from t order by name"
for id in t.getRowIds('name'):
    print t.getRow(id)

print "=== select * from t where age >= 30 order by age"
for id in t.getRowIds('age', 30):
    print t.getRow(id)

print "=== select * from t where 35 <= age < 70 order by age desc"
for id in reversed(t.getRowIds('age', 35, 70)):
    print t.getRow(id)

print "=== select * from t where age < 40"
ids = t.getRowIds('age', None, 40)
for id in ids:
    print t.getRow(id)

print "=== delete from t where age >= 40"
for id in ids:
    t.deleteRow(id)

print "=== select * from t order by age desc"
for id in reversed(t.getRowIds('age')):
    print t.getRow(id)

