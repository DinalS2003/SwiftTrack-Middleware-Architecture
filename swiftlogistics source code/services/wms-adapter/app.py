"""
WMS Adapter - SwiftLogistics
Adapter for Warehouse Management System (Proprietary TCP/IP Protocol)
Handles package tracking and warehouse operations
"""

from flask import Flask, request, jsonify
from datetime import datetime
import logging
import socket
import threading
import json

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock WMS database
warehouse_packages = {}
warehouse_zones = {
    'ZONE_A': {'capacity': 1000, 'current': 0, 'type': 'receiving'},
    'ZONE_B': {'capacity': 2000, 'current': 0, 'type': 'storage'},
    'ZONE_C': {'capacity': 500, 'current': 0, 'type': 'loading'}
}

# TCP/IP Server configuration
TCP_HOST = '0.0.0.0'
TCP_PORT = 9999
tcp_server = None

class WMSTCPProtocol:
    """
    Proprietary TCP/IP protocol handler for WMS
    Message format: <CMD>|<DATA>|<CHECKSUM>
    """
    
    @staticmethod
    def encode_message(command, data):
        """Encode message in proprietary format"""
        data_str = json.dumps(data)
        checksum = sum(ord(c) for c in data_str) % 256
        message = f"{command}|{data_str}|{checksum}\n"
        return message.encode('utf-8')
    
    @staticmethod
    def decode_message(message):
        """Decode proprietary message format"""
        try:
            parts = message.decode('utf-8').strip().split('|')
            if len(parts) != 3:
                return None
            
            command = parts[0]
            data = json.loads(parts[1])
            checksum = int(parts[2])
            
            # Verify checksum
            calculated_checksum = sum(ord(c) for c in parts[1]) % 256
            if calculated_checksum != checksum:
                logger.error("Checksum verification failed")
                return None
            
            return {'command': command, 'data': data}
        except Exception as e:
            logger.error(f"Error decoding message: {str(e)}")
            return None
    
    @staticmethod
    def create_response(status, data):
        """Create response message"""
        return WMSTCPProtocol.encode_message('RESPONSE', {
            'status': status,
            'data': data,
            'timestamp': datetime.utcnow().isoformat()
        })

def tcp_server_handler(client_socket, address):
    """Handle TCP client connections"""
    logger.info(f"TCP client connected from {address}")
    
    try:
        while True:
            message = client_socket.recv(4096)
            if not message:
                break
            
            decoded = WMSTCPProtocol.decode_message(message)
            if decoded:
                command = decoded['command']
                data = decoded['data']
                
                logger.info(f"Received TCP command: {command}")
                
                # Process command
                if command == 'ADD_PACKAGE':
                    response = handle_add_package_tcp(data)
                elif command == 'GET_PACKAGE':
                    response = handle_get_package_tcp(data)
                elif command == 'UPDATE_STATUS':
                    response = handle_update_status_tcp(data)
                else:
                    response = WMSTCPProtocol.create_response('ERROR', {'error': 'Unknown command'})
                
                client_socket.send(response)
    
    except Exception as e:
        logger.error(f"TCP handler error: {str(e)}")
    
    finally:
        client_socket.close()
        logger.info(f"TCP client disconnected from {address}")

def handle_add_package_tcp(data):
    """Handle ADD_PACKAGE TCP command"""
    try:
        order_id = data['order_id']
        packages = data['packages']
        
        for pkg in packages:
            tracking_id = pkg['tracking_id']
            warehouse_packages[tracking_id] = {
                'tracking_id': tracking_id,
                'order_id': order_id,
                'weight': pkg.get('weight', 0),
                'zone': 'ZONE_A',
                'status': 'received',
                'received_at': datetime.utcnow().isoformat()
            }
        
        return WMSTCPProtocol.create_response('SUCCESS', {
            'order_id': order_id,
            'packages_added': len(packages)
        })
    
    except Exception as e:
        return WMSTCPProtocol.create_response('ERROR', {'error': str(e)})

def handle_get_package_tcp(data):
    """Handle GET_PACKAGE TCP command"""
    tracking_id = data.get('tracking_id')
    
    if tracking_id in warehouse_packages:
        return WMSTCPProtocol.create_response('SUCCESS', warehouse_packages[tracking_id])
    else:
        return WMSTCPProtocol.create_response('ERROR', {'error': 'Package not found'})

def handle_update_status_tcp(data):
    """Handle UPDATE_STATUS TCP command"""
    tracking_id = data.get('tracking_id')
    status = data.get('status')
    
    if tracking_id in warehouse_packages:
        warehouse_packages[tracking_id]['status'] = status
        warehouse_packages[tracking_id]['updated_at'] = datetime.utcnow().isoformat()
        
        return WMSTCPProtocol.create_response('SUCCESS', warehouse_packages[tracking_id])
    else:
        return WMSTCPProtocol.create_response('ERROR', {'error': 'Package not found'})

def start_tcp_server():
    """Start TCP server for proprietary protocol"""
    global tcp_server
    
    try:
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((TCP_HOST, TCP_PORT))
        tcp_server.listen(5)
        
        logger.info(f"WMS TCP server listening on {TCP_HOST}:{TCP_PORT}")
        
        while True:
            client_socket, address = tcp_server.accept()
            client_thread = threading.Thread(
                target=tcp_server_handler,
                args=(client_socket, address)
            )
            client_thread.daemon = True
            client_thread.start()
    
    except Exception as e:
        logger.error(f"TCP server error: {str(e)}")

# REST API endpoints (for HTTP access to WMS)
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'wms-adapter'})

@app.route('/add_packages', methods=['POST'])
def add_packages():
    """
    Add packages to WMS (via REST, internally uses TCP/IP)
    """
    try:
        data = request.json
        order_id = data['order_id']
        packages = data.get('packages', [])
        
        logger.info(f"Adding {len(packages)} packages for order {order_id} to WMS")
        
        # Simulate TCP/IP communication
        for pkg in packages:
            tracking_id = pkg.get('tracking_id', f"PKG-{order_id}-{len(warehouse_packages)}")
            
            warehouse_packages[tracking_id] = {
                'tracking_id': tracking_id,
                'order_id': order_id,
                'weight': pkg.get('weight', 0),
                'dimensions': pkg.get('dimensions', {}),
                'zone': 'ZONE_A',
                'status': 'received',
                'received_at': datetime.utcnow().isoformat()
            }
            
            warehouse_zones['ZONE_A']['current'] += 1
        
        logger.info(f"Packages added to WMS via TCP/IP protocol")
        
        return jsonify({
            'success': True,
            'order_id': order_id,
            'packages_added': len(packages),
            'tracking_ids': [pkg['tracking_id'] for pkg in warehouse_packages.values() if pkg['order_id'] == order_id]
        })
    
    except Exception as e:
        logger.error(f"Error adding packages to WMS: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/packages/<order_id>', methods=['DELETE'])
def remove_packages(order_id):
    """Remove packages from WMS (compensation)"""
    try:
        logger.info(f"Removing packages for order {order_id} from WMS")
        
        removed_count = 0
        tracking_ids_to_remove = [
            tid for tid, pkg in warehouse_packages.items()
            if pkg['order_id'] == order_id
        ]
        
        for tracking_id in tracking_ids_to_remove:
            zone = warehouse_packages[tracking_id]['zone']
            warehouse_zones[zone]['current'] -= 1
            del warehouse_packages[tracking_id]
            removed_count += 1
        
        return jsonify({
            'success': True,
            'order_id': order_id,
            'packages_removed': removed_count
        })
    
    except Exception as e:
        logger.error(f"Error removing packages from WMS: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/package/<tracking_id>', methods=['GET'])
def get_package(tracking_id):
    """Get package details"""
    if tracking_id in warehouse_packages:
        return jsonify(warehouse_packages[tracking_id])
    else:
        return jsonify({'error': 'Package not found'}), 404

@app.route('/package/<tracking_id>/move', methods=['PUT'])
def move_package(tracking_id):
    """Move package to different zone"""
    try:
        data = request.json
        new_zone = data.get('zone')
        
        if tracking_id in warehouse_packages:
            old_zone = warehouse_packages[tracking_id]['zone']
            
            # Update zone counts
            warehouse_zones[old_zone]['current'] -= 1
            warehouse_zones[new_zone]['current'] += 1
            
            warehouse_packages[tracking_id]['zone'] = new_zone
            warehouse_packages[tracking_id]['moved_at'] = datetime.utcnow().isoformat()
            
            logger.info(f"Package {tracking_id} moved from {old_zone} to {new_zone}")
            
            return jsonify({
                'success': True,
                'tracking_id': tracking_id,
                'zone': new_zone
            })
        else:
            return jsonify({'success': False, 'error': 'Package not found'}), 404
    
    except Exception as e:
        logger.error(f"Error moving package: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/package/<tracking_id>/status', methods=['PUT'])
def update_package_status(tracking_id):
    """Update package status"""
    try:
        data = request.json
        status = data.get('status')
        
        if tracking_id in warehouse_packages:
            warehouse_packages[tracking_id]['status'] = status
            warehouse_packages[tracking_id]['updated_at'] = datetime.utcnow().isoformat()
            
            return jsonify({
                'success': True,
                'tracking_id': tracking_id,
                'status': status
            })
        else:
            return jsonify({'success': False, 'error': 'Package not found'}), 404
    
    except Exception as e:
        logger.error(f"Error updating package status: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/zones', methods=['GET'])
def get_zones():
    """Get warehouse zones status"""
    return jsonify({'zones': warehouse_zones})

@app.route('/packages/ready-for-loading', methods=['GET'])
def get_packages_ready_for_loading():
    """Get packages ready for loading onto vehicles"""
    ready_packages = [
        pkg for pkg in warehouse_packages.values()
        if pkg['zone'] == 'ZONE_C' and pkg['status'] == 'ready'
    ]
    
    return jsonify({
        'packages': ready_packages,
        'count': len(ready_packages)
    })

if __name__ == '__main__':
    # Start TCP server in background thread
    tcp_thread = threading.Thread(target=start_tcp_server)
    tcp_thread.daemon = True
    tcp_thread.start()
    
    logger.info("Starting WMS Adapter (TCP/IP + REST) on port 5004")
    app.run(host='0.0.0.0', port=5004, debug=True)
