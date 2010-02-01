#
# effyTable: simple table database module.
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

import bisect

class effyTable:
    __indexes = {}
    __rows = {}
    __maxrowid = 0

    def addRow(self, row):
        self.__maxrowid += 1
        self.__rows[self.__maxrowid] = row
        for col in self.__indexes:
            if col in row:
                bisect.insort(self.__indexes[col], (row[col], self.__maxrowid))

    def getRowIds(self, col, value):
        if col in self.__indexes:
            i = bisect.bisect_left(self.__indexes[col], (value,))
            return [index_row[1] for index_row in self.__indexes[col][i:]]

    def getRow(self, rowid):
        try:
            return self.__rows[rowid]
        except IndexError:
            return None

    def deleteRow(self, rowid):
        row = self.__rows[rowid]
        for col in self.__indexes:
            if col in row:
                del self.__indexes[col][bisect.bisect_left(self.__indexes[col], (row[col], rowid))]
        del self.__rows[rowid]

    def setIndex(self, col):
        i = []
        if col not in self.__indexes:
            for rowid,row in self.__rows.items():
                if col in row:
                    i.append((row[col],rowid))
            i.sort()
            self.__indexes[col] = i
        return len(i)

    def removeIndex(self, col):
        if col in self.__indexes:
            del self.indexes[col]

    def getIndexNames(self):
        return self.__indexes.keys()

    def show(self):
        print "rows:", self.__rows
        print "indexes:", self.__indexes
        print "maxrowid:", self.__maxrowid
