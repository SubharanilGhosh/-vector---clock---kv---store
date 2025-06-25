import requests
import time

node0 = 'http://localhost:5000'
node1 = 'http://localhost:5001'
node2 = 'http://localhost:5002'

print("\n--- Testing Causal Consistency ---\n")

print("Step 1: Writing 'A' to key 'x' at node0")
response1 = requests.post(f'{node0}/write', json={'key': 'x', 'value': 'A'})
print("Response:", response1.json())
time.sleep(2)

print("\nStep 2: Reading key 'x' at node1")
response2 = requests.get(f'{node1}/get/x')
print("Response:", response2.json())

print("\nStep 3: Writing 'B' to key 'x' at node2 (causal dependency)")
response3 = requests.post(f'{node2}/write', json={'key': 'x', 'value': 'B'})
print("Response:", response3.json())
time.sleep(2)

print("\nStep 4: Final status of all nodes")
for i, url in enumerate([node0, node1, node2]):
    print(f"\nNode{i} /status:")
    try:
        response = requests.get(f'{url}/status')
        print(response.json())
    except Exception as e:
        print(f"Error connecting to node{i}: {e}")
