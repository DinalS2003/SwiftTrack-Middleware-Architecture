"""
API Gateway for SwiftLogistics
Entry point for all client and driver applications
Implements routing, authentication, and WebSocket support
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import jwt
import requests
from datetime import datetime, timedelta
import logging
from functools import wraps

app = Flask(__name__)
app.config['SECRET_KEY'] = 'swiftlogistics-secret-key-change-in-production'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

SERVICES = {
    'order_service': 'http://order-service:5001',
    'cms_adapter': 'http://cms-adapter:5002',
    'ros_adapter': 'http://ros-adapter:5003',
    'wms_adapter': 'http://wms-adapter:5004'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
connected_clients = {}

# Authentication decorator
def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'No authorization token provided'}), 401
        
        try:
            if token.startswith('Bearer '):
                token = token[7:]
            
            payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            request.user = payload
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
        
        return f(*args, **kwargs)
    return decorated_function

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'services': check_services_health()
    })

def check_services_health():
    health_status = {}
    for service_name, service_url in SERVICES.items():
        try:
            response = requests.get(f"{service_url}/health", timeout=2)
            health_status[service_name] = 'healthy' if response.status_code == 200 else 'unhealthy'
        except:
            health_status[service_name] = 'unreachable'
    return health_status

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Authenticate user and return JWT token"""
    data = request.json
    username = data.get('username')
    password = data.get('password')
    user_type = data.get('user_type', 'client')
    
    if username and password:
        payload = {
            'username': username,
            'user_type': user_type,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')
        
        logger.info(f"User {username} logged in as {user_type}")
        
        return jsonify({
            'token': token,
            'user_type': user_type,
            'username': username
        })
    
    return jsonify({'error': 'Invalid credentials'}), 401

# Order Management APIs (Client Portal)
@app.route('/api/orders', methods=['POST'])
@require_auth
def create_order():
    """Submit a new order"""
    try:
        order_data = request.json
        order_data['client_id'] = request.user.get('username')
        
        logger.info(f"Creating order for client {order_data['client_id']}")
        
        response = requests.post(
            f"{SERVICES['order_service']}/orders",
            json=order_data,
            timeout=10
        )
        
        if response.status_code == 201:
            result = response.json()
            
            socketio.emit('order_created', {
                'order_id': result['order_id'],
                'status': 'processing',
                'timestamp': datetime.utcnow().isoformat()
            }, room=order_data['client_id'])
            
            return jsonify(result), 201
        else:
            return jsonify({'error': 'Failed to create order'}), response.status_code
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error creating order: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/orders/<order_id>', methods=['GET'])
@require_auth
def get_order(order_id):
    """Get order status"""
    try:
        response = requests.get(
            f"{SERVICES['order_service']}/orders/{order_id}",
            timeout=5
        )
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({'error': 'Order not found'}), 404
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error retrieving order: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/orders', methods=['GET'])
@require_auth
def list_orders():
    """List all orders for the authenticated client"""
    try:
        client_id = request.user.get('username')
        
        response = requests.get(
            f"{SERVICES['order_service']}/orders",
            params={'client_id': client_id},
            timeout=5
        )
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({'error': 'Failed to retrieve orders'}), response.status_code
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error listing orders: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/driver/manifest', methods=['GET'])
@require_auth
def get_driver_manifest():
    """Get driver's delivery manifest"""
    try:
        driver_id = request.user.get('username')
        date = request.args.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
        
        logger.info(f"Fetching manifest for driver {driver_id} on {date}")
        
        response = requests.get(
            f"{SERVICES['order_service']}/driver/manifest",
            params={'driver_id': driver_id, 'date': date},
            timeout=5
        )
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({'error': 'Failed to retrieve manifest'}), response.status_code
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error retrieving manifest: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/orders/<order_id>/status', methods=['PUT'])
@require_auth
def update_order_status(order_id):
    try:
        driver_id = request.user.get('username')
        update_data = request.json
        
        logger.info(f"Driver {driver_id} updating order {order_id} status to {update_data.get('status')}")
        
        response = requests.put(
            f"{SERVICES['order_service']}/orders/{order_id}/status",
            json=update_data,
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Emit real-time update to client - broadcast to specific client room
            if 'client_id' in result:
                client_id = result['client_id']
                notification_data = {
                    'order_id': order_id,
                    'status': update_data.get('status'),
                    'notes': update_data.get('notes', ''),
                    'timestamp': datetime.utcnow().isoformat(),
                    'driver_id': driver_id
                }
                
                socketio.emit('order_status_updated', notification_data, room=client_id)
                
                logger.info(f"Notification sent to client {client_id} for order {order_id}")
            
            return jsonify(result)
        else:
            return jsonify({'error': 'Failed to update order status'}), response.status_code
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error updating order status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/driver/delivery/<delivery_id>', methods=['PUT'])
@require_auth
def update_delivery_status(delivery_id):
    try:
        driver_id = request.user.get('username')
        update_data = request.json
        update_data['driver_id'] = driver_id
        
        logger.info(f"Driver {driver_id} updating delivery {delivery_id}")
        
        response = requests.put(
            f"{SERVICES['order_service']}/delivery/{delivery_id}",
            json=update_data,
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Emit real-time update to client - broadcast to specific client room
            if 'client_id' in result:
                client_id = result['client_id']
                notification_data = {
                    'delivery_id': delivery_id,
                    'order_id': result.get('order_id'),
                    'status': update_data.get('status'),
                    'notes': update_data.get('notes', ''),
                    'timestamp': datetime.utcnow().isoformat(),
                    'driver_id': driver_id
                }
                
                socketio.emit('delivery_updated', notification_data, room=client_id)
                
                logger.info(f"Notification sent to client {client_id} for delivery {delivery_id}")
            
            return jsonify(result)
        else:
            return jsonify({'error': 'Failed to update delivery'}), response.status_code
    
    except requests.Timeout:
        return jsonify({'error': 'Order service timeout'}), 504
    except Exception as e:
        logger.error(f"Error updating delivery: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/driver/proof', methods=['POST'])
@require_auth
def upload_proof_of_delivery():
    """Upload proof of delivery (signature/photo)"""
    try:
        driver_id = request.user.get('username')
        content_type = request.content_type
        
        # Support both multipart/form-data (file upload) and JSON (base64)
        if 'application/json' in content_type:
            # Base64 encoded signature
            data = request.json
            delivery_id = data.get('delivery_id')
            proof_type = data.get('proof_type', 'signature')
            proof_data = data.get('data')  # Base64 encoded image
            
            if not delivery_id or not proof_data:
                return jsonify({'error': 'Missing delivery_id or proof data'}), 400
            
            logger.info(f"Driver {driver_id} uploaded {proof_type} for {delivery_id}")
            
            # In production: decode base64 and upload to S3/Cloud Storage
            # proof_bytes = base64.b64decode(proof_data.split(',')[1])
            # upload_to_storage(proof_bytes, f"{delivery_id}-{proof_type}.png")
            
            return jsonify({
                'delivery_id': delivery_id,
                'proof_type': proof_type,
                'driver_id': driver_id,
                'status': 'uploaded',
                'timestamp': datetime.utcnow().isoformat()
            })
        else:
            # Multipart file upload
            delivery_id = request.form.get('delivery_id')
            proof_type = request.form.get('proof_type', 'photo')
            file = request.files.get('file')
            
            if not delivery_id or not file:
                return jsonify({'error': 'Missing delivery_id or file'}), 400
            
            # In production: upload to cloud storage (S3, etc.)
            # filename = secure_filename(f"{delivery_id}-{proof_type}-{datetime.utcnow().timestamp()}.jpg")
            # upload_to_storage(file, filename)
            
            logger.info(f"Driver {driver_id} uploaded {proof_type} file for {delivery_id}")
            
            return jsonify({
                'delivery_id': delivery_id,
                'proof_type': proof_type,
                'driver_id': driver_id,
                'status': 'uploaded',
                'timestamp': datetime.utcnow().isoformat()
            })
    
    except Exception as e:
        logger.error(f"Error uploading proof: {str(e)}")
        return jsonify({'error': str(e)}), 500

# WebSocket events for real-time updates
@socketio.on('connect')
def handle_connect():
    logger.info(f"Client connected: {request.sid}")
    emit('connected', {'message': 'Connected to SwiftLogistics'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('subscribe')
def handle_subscribe(data):
    """Subscribe to updates for specific client/driver"""
    user_id = data.get('user_id')
    if user_id:
        from flask_socketio import join_room
        join_room(user_id)
        connected_clients[request.sid] = user_id
        logger.info(f"Client {request.sid} subscribed as {user_id} and joined room")
        emit('subscribed', {'user_id': user_id})

@socketio.on('unsubscribe')
def handle_unsubscribe():
    """Handle client unsubscription from WebSocket notifications."""
    if request.sid in connected_clients:
        user_id = connected_clients.pop(request.sid)
        leave_room(user_id)
        logger.info(f"Client {request.sid} unsubscribed from {user_id}")
        emit('unsubscribed', {'user_id': user_id})

# Static file serving for web UIs
@app.route('/')
def serve_dashboard():
    """Serve the main dashboard landing page"""
    return send_from_directory('.', 'index.html')

@app.route('/client-portal')
@app.route('/client-portal/')
def serve_client_portal():
    return send_from_directory('static/client-portal', 'index.html')

@app.route('/driver-app')
@app.route('/driver-app/')
def serve_driver_app():
    return send_from_directory('static/driver-app', 'index.html')

@app.route('/static/<path:path>')
def serve_static(path):
    """Serve static files"""
    return send_from_directory('static', path)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    logger.info("Starting SwiftLogistics API Gateway on port 5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
