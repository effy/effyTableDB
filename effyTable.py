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
from multiprocessing import Lock

class effyTable:
    __indexes = {}
    __rows = {}
    __maxrowid = 0
    __lock = Lock()

    def __setRow(self, rowid, row):
        self.__rows[rowid] = row
        for col in self.__indexes:
            if col in row:
                self.__lock.acquire()
                bisect.insort(self.__indexes[col], (row[col], rowid))
                self.__lock.release()
        
    def addRow(self, row):
        self.__lock.acquire()
        self.__maxrowid += 1
        rowid = self.__maxrowid
        self.__lock.release()
        self.__setRow(rowid, row)
        return rowid

    def getRowIds(self, col, from_value=None, to_value=None):
        if col in self.__indexes:
            from_i = bisect.bisect_left(self.__indexes[col], (from_value,))
            if to_value:
                to_i = bisect.bisect_left(self.__indexes[col], (to_value,))
                return [index_row[1] for index_row in self.__indexes[col][from_i:to_i]]
            else: 
                return [index_row[1] for index_row in self.__indexes[col][from_i:]]

    def getRow(self, rowid):
        try:
            return self.__rows[rowid].copy()
        except IndexError:
            return None

    def deleteRow(self, rowid):
        self.__lock.acquire()
        row = self.__rows[rowid]
        for col in self.__indexes:
            if col in row:
                del self.__indexes[col][bisect.bisect_left(self.__indexes[col], (row[col], rowid))]
        del self.__rows[rowid]
        self.__lock.release()

    def updateRow(self, rowid, row):
        if rowid in self.__rows:
            self.deleteRow(rowid)
        self.__setRow(rowid, row)

    def setIndex(self, col):
        i = []
        if col not in self.__indexes:
            self.__lock.acquire()
            for rowid,row in self.__rows.items():
                if col in row:
                    i.append((row[col],rowid))
            i.sort()
            self.__indexes[col] = i
            self.__lock.release()
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
