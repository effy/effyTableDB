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
from multiprocessing.managers import BaseManager

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

class PeerAddresses:
    __addresses = effyTable()

    def __init__(self):
        self.__addresses.setIndex('key')
        self.__addresses.setIndex('address')
        
    def getAddress(self, key):
        id = self.__addresses.getRowIds('key', None, key)[-1]
        row = self.__addresses.getRow(id)
        return row['address']

    def setAddress(self, key, address):
        rows = self.__addresses.getRowIds('address', address)
        if len(rows) > 0 and self.__addresses.getRow(rows[0])['address'] == address:
            self.__addresses.updateRow(rows[0], {'key':key, 'address':address})
        else:
            self.__addresses.addRow({'key':key, 'address':address})

"""
usage of class Peer:

*** at manager node
from effyTable import Peer
m=Peer(address=('localhost',1000), authkey='a')
m.startManager()

*** at node A
from effyTable import Peer
m=Peer(address=('localhost',1001), authkey='a')
m.connectManager(address=('localhost', 1000))
m.startPeer(key=None)

*** at node B
from effyTable import Peer
m=Peer(address=('localhost',1002), authkey='a')
m.connectManager(address=('localhost', 1000))
m.startPeer(key='effy')

*** at node A or B
m.getTable('effy').setIndex('key')
m.getTable('effy1').setIndex('key')

(key,value) = ('sample', 12345)
m.getTable(key).addRow({'key':key, 'value':value})
"""

class Peer(BaseManager):
    __manager = None
    __addresses = None

    __table = None
    __peers = None

    __address = None
    __authkey = None

    def __init__(self, address, authkey):
        self.__address = address
        self.__authkey = authkey
        BaseManager.__init__(self, address, authkey)

    def startManager(self):
        self.__addresses = PeerAddresses()
        Peer.register('addresses', callable=lambda:self.__addresses)
        self.start()

    def connectManager(self, address):
        Peer.register('addresses')
        self.__manager = Peer(address=address, authkey=self.__authkey)
        self.__manager.connect()
        self.__addresses = self.__manager.addresses()
    
    def startPeer(self, key):
        self.__addresses.setAddress(key, self.__address)
        self.__table = effyTable()
        self.__peers = {}
        Peer.register('table', callable=lambda:self.__table)
        self.start()
   
    def getTable(self, key):
        address = self.__addresses.getAddress(key)
        if address not in self.__peers:
            Peer.register('table')
            peer = Peer(address=address, authkey=self.__authkey)
            peer.connect()
            self.__peers[address] = peer
        else:
            peer = self.__peers[address]

        return peer.table()
 
