import requests

response = requests.post('http://localhost:5000/add-points', json={
    "points": [
        {"lat": 37.7749, "long": -122.4194, "name": "San Francisco"},
        {"lat": 40.7128, "long": -74.0060, "name": "New York"}
    ]
})
print(response.json())
