from flask import Flask, jsonify
import json
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

@app.route('/macd')
def get_macd_result():
    with open('macd_result.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10015)
