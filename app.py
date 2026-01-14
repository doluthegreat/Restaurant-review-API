from flask import Flask, jsonify, request

app = Flask(__name__)

# Temporary in-memory data (we'll replace with database later)
restaurants = []

@app.route('/')
def home():
    return jsonify({"message": "Restaurant Review API is running!"})

@app.route('/restaurants', methods=['GET'])
def get_restaurants():
    return jsonify({"restaurants": restaurants})

@app.route('/restaurants', methods=['POST'])
def add_restaurant():
    data = request.get_json()
    
    restaurant = {
        "id": len(restaurants) + 1,
        "name": data.get("name"),
        "location": data.get("location")
    }
    
    restaurants.append(restaurant)
    return jsonify(restaurant), 201

if __name__ == '__main__':
    app.run(debug=True)