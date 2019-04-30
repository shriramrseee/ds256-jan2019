from flask import Flask, request, current_app, Blueprint
from flask_cors import CORS
import json

from input_query import process_input_query

app = Blueprint('kg_app', 'kg_api', url_prefix='/')


def create_app(cache, cutv):
    rest_app = Flask('kg_api')
    rest_app.config['cache'] = cache
    rest_app.config['cutv'] = cutv
    CORS(rest_app)
    rest_app.register_blueprint(app)
    return rest_app


@app.route('query', methods=['POST'])
def query():
    """
    Get query response
    """
    d = request.get_json()
    result = process_input_query(d, current_app.config['cache'], current_app.config['cutv'])
    return json.dumps(result)