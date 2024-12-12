import logging
import json
from kafka import KafkaConsumer
from datetime import datetime
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
 
# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'testTopic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
 
# Initialize data for plotting
timestamps = []
view_differences = []
 
# Initialize Dash app
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Live View Count Updates"),
    dcc.Graph(id='live-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1000,  # in milliseconds
        n_intervals=0
    )
])
 
# Callback to update the graph
@app.callback(
    Output('live-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    # Create the figure
    fig = {
        'data': [
            {
                'x': timestamps,
                'y': view_differences,
                'type': 'scatter',
                'mode': 'markers+lines',
                'name': 'View Difference'
            }
        ],
        'layout': {
            'title': 'Live View Count Updates',
            'xaxis': {'title': 'Timestamp'},
            'yaxis': {'title': 'View Difference'},
            'autosize': True
        }
    }
    return fig
 
# Kafka consumer in a separate thread
import threading
 
def consume_kafka():
    logger.info("Starting Kafka consumer...")
    try:
        for message in consumer:
            # Extract data from the Kafka message
            data = message.value
            view_diff = data['viewDifference']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Append new data
            timestamps.append(timestamp)
            view_differences.append(view_diff)
 
            # Keep data size manageable
            if len(timestamps) > 1000:
                timestamps.pop(0)
                view_differences.pop(0)
 
            # Log the consumed message
            logger.info(f"Consumed: Timestamp: {timestamp}, View Difference: {view_diff}")
 
    except Exception as e:
        logger.error(f"Error while consuming: {e}")
 
    finally:
        logger.info("Consumer stopped.")
 
# Run the Kafka consumer in a separate thread
thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()
 
# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=False)