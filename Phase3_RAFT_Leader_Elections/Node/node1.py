import json
import socket
import time
import random

class Node:
    def __init__(self, id):
        self.id = id

        # required from the handout
        self.cur_term = 0
        self.voted_for = None
        self.log = []
        self.timeout = 300
        self.heartbeat = 50
        self.role = "follower"

        self.next_election_time = time.time() + self.timeout
        self.vote = 0

        # msg send and recv
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind((self.address,5555))
        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def listener(self,data):

            if self.role == 'folllower':
                follower(data)
            elif self.role == 'candidate':
                candidate(data)
            elif self.role == 'leader':
                leader(data)





    def send(self, msg, addr):
        msg = json.dumps(msg).encode('utf-8')
        self.ss.sendto(msg, (addr,5555))

    def recv(self):
        msg, addr = self.cs.recvfrom(5555)
        return json.loads(msg), addr

    def follower(self,data):

        if data is None or time.time() > self.next_election_time:
            # required from the handout
            self.cur_term = self.cur_term + 1
            self.voted_for = self.id
            self.role = "candidate"
            self.vote = 1

            request = {
                'sender_name': self.id,
                'request': 'VOTE_REQUEST',
                'term': self.cur_term,
                'key': self.role,
                'value': {}
            }
            candidate(request)

        else:
            if data['type'] == 'APPEND_RPC':

                if data['term'] >= self.cur_term:
                    self.cur_term = data['term']
                    self.next_leader_election_time = t + self.timeout_interval


            elif data['type'] == 'VOTE_REQUEST':
                if data['term'] > self.cur_term and self.voted_for is None:

                    self.cur_term = data['term']
                    self.voted_for = data['sender_name']
                    request = {
                        'sender_name': self.id,
                        'request': 'VOTE_ACK',
                        'term': self.cur_term,
                        'key': self.role,
                        'value': {}
                    }
                    send(request, data['sender_name'])



    def candidate(self,data):
        l = ['node1', 'node2', 'node3','node4','node5']
        l.remove(self.id)
        if data['request'] == 'VOTE_REQUEST':
            if data['sender_name'] == self.id and data['term'] >= self.cur_term:
                for id in l:
                    self.send(data, id)

            elif data['sender_name'] != self.id and data['term'] > self.cur_term:

                # required from the handout
                self.cur_term = data['term']
                self.voted_for = data['sender_name']
                self.role = "follower"
                self.vote = 0
                request = {
                    'sender_name': self.id,
                    'request': 'VOTE_ACK',
                    'term': self.cur_term,
                    'key': self.role,
                    'value': {}
                }
                send(request,data['sender_name'])
        if data['request'] == 'VOTE_ACK':
            if data['term'] == self.cur_term:

                self.vote = self.vote + 1
                if self.vote > (len(l)+1)/2:

                    self.role = "leader"
                    self.next_election_time = time.time() + self.timeout
                    self.vote = 0
                    self.leader(data=None)
        if data['request'] == 'APPEND_RPC':
            if data['term'] > self.cur_term:

                self.cur_term = data['term']
                self.voted_for = None
                self.role = "follower"
                self.next_election_time = time.time() + self.timeout
                self.vote = 0

    def leader(self,data):
        if data is None:

            count = 0
            l = ['node1', 'node2', 'node3']
            l.remove(self.id)

            if count == 0:
                for id in l:
                    request = {
                               'sender_name': self.id,
                               'request': 'APPEND_RPC',
                               'term': self.cur_term,
                               'key': self.role,
                               'value': {}
                               }

                    self.send(request, id)
                count = count + 1
            while count>0:
                time.sleep(self.heartbeat)
                for id in l:
                    request = {
                        'sender_name': self.id,
                        'request': 'APPEND_RPC',
                        'term': self.cur_term,
                        'key': self.role,
                        'value': {}
                    }

                    self.send(request, id)
                count = count + 1
        else:
            if data['term'] > self.cur_term:
                if data['request'] == 'APPEND_RPC':
                    self.cur_term = data['term']
                    self.voted_for = None
                    self.role = "follower"
                    self.next_election_time = time.time() + self.timeout
                    self.vote = 0
                    self.follower(data)
                elif data['request'] == 'VOTE_REQUEST':
                    self.voted_for = None
                    self.role = "follower"
                    self.follower(data)



    def run(self):
        while True:
            data, address = self.recv()
            self.listener(data)

if __name__ == "__main__":
    node = Node
    threading.Thread(target=node.run).start()











