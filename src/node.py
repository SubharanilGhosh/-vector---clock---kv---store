from flask import Flask, request, jsonify
import sys

app = Flask(__name__)

node_id = int(sys.argv[1])
total_nodes = 3
vector_clock = [0] * total_nodes
kv_store = {}
buffer = []

peers = {
    0: 'http://node0:5000',
    1: 'http://node1:5000',
    2: 'http://node2:5000'
}

def can_deliver(msg_vc, sender):
    for i in range(total_nodes):
        if i == sender:
            if msg_vc[i] != vector_clock[i] + 1:
                return False
        else:
            if msg_vc[i] > vector_clock[i]:
                return False
    return True

def try_deliver():
    global buffer
    delivered = []
    for msg in buffer:
        key, val, msg_vc, sender = msg
        if can_deliver(msg_vc, sender):
            kv_store[key] = val
            for i in range(total_nodes):
                vector_clock[i] = max(vector_clock[i], msg_vc[i])
            delivered.append(msg)
    for d in delivered:
        buffer.remove(d)

@app.route('/put', methods=['POST'])
def put():
    data = request.json
    key = data['key']
    value = data['value']
    sender = data['sender']
    msg_vc = data['vector_clock']

    if can_deliver(msg_vc, sender):
        kv_store[key] = value
        for i in range(total_nodes):
            vector_clock[i] = max(vector_clock[i], msg_vc[i])
        try_deliver()
        return jsonify({'status': 'delivered'})
    else:
        buffer.append((key, value, msg_vc, sender))
        return jsonify({'status': 'buffered'})

@app.route('/write', methods=['POST'])
def write():
    data = request.json
    key = data['key']
    value = data['value']
    vector_clock[node_id] += 1
    kv_store[key] = value

    import requests
    for i in range(total_nodes):
        if i != node_id:
            try:
                requests.post(f"{peers[i]}/put", json={
                    'key': key,
                    'value': value,
                    'vector_clock': vector_clock.copy(),
                    'sender': node_id
                })
            except:
                continue

    return jsonify({'status': 'written', 'vector_clock': vector_clock})

@app.route('/get/<key>', methods=['GET'])
def get(key):
    val = kv_store.get(key, None)
    return jsonify({'value': val, 'vector_clock': vector_clock})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({'store': kv_store, 'vector_clock': vector_clock, 'buffer': buffer})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
