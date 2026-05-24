"""
ROS Adapter - SwiftLogistics
Adapter for cloud-based Route Optimization System (REST/JSON)
Handles route planning and optimization
"""

from flask import Flask, request, jsonify
from datetime import datetime
import logging
import random

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock ROS database
delivery_points = {}
routes = {}
vehicles = {
    'VEH001': {'driver_id': 'DRIVER001', 'capacity': 50, 'status': 'available'},
    'VEH002': {'driver_id': 'DRIVER002', 'capacity': 50, 'status': 'available'},
    'VEH003': {'driver_id': 'DRIVER003', 'capacity': 30, 'status': 'available'}
}

def calculate_route(points):
    """Mock route optimization algorithm"""
    # In production, use Google Maps API, MapBox, or custom algorithm
    
    total_distance = round(random.uniform(10.0, 50.0), 2)
    total_time = round(total_distance * 4.5, 0)  # ~4.5 min per km
    
    optimized_sequence = points.copy()
    random.shuffle(optimized_sequence)
    
    return {
        'total_distance_km': total_distance,
        'estimated_time_minutes': total_time,
        'waypoints': optimized_sequence,
        'optimization_score': round(random.uniform(0.85, 0.98), 2)
    }

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'ros-adapter'})

@app.route('/add_delivery_point', methods=['POST'])
def add_delivery_point():
    """
    Add delivery point to ROS for route optimization
    REST/JSON endpoint
    """
    try:
        data = request.json
        order_id = data['order_id']
        address = data['address']
        priority = data.get('priority', 'normal')
        
        logger.info(f"Adding delivery point for order {order_id} to ROS")
        
        # Generate coordinates (mock geocoding)
        latitude = round(6.9271 + random.uniform(-0.1, 0.1), 6)  # Colombo area
        longitude = round(79.8612 + random.uniform(-0.1, 0.1), 6)
        
        delivery_points[order_id] = {
            'order_id': order_id,
            'address': address,
            'coordinates': {
                'latitude': latitude,
                'longitude': longitude
            },
            'priority': priority,
            'status': 'pending',
            'added_at': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Delivery point added: {order_id} at ({latitude}, {longitude})")
        
        return jsonify({
            'success': True,
            'order_id': order_id,
            'coordinates': delivery_points[order_id]['coordinates'],
            'message': 'Delivery point added to optimization queue'
        })
    
    except Exception as e:
        logger.error(f"Error adding delivery point: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/delivery_point/<order_id>', methods=['DELETE'])
def remove_delivery_point(order_id):
    """Remove delivery point from ROS (compensation)"""
    try:
        logger.info(f"Removing delivery point for order {order_id} from ROS")
        
        if order_id in delivery_points:
            del delivery_points[order_id]
            return jsonify({'success': True, 'order_id': order_id, 'message': 'Delivery point removed'})
        else:
            return jsonify({'success': False, 'error': 'Delivery point not found'}), 404
    
    except Exception as e:
        logger.error(f"Error removing delivery point: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/optimize_route', methods=['POST'])
def optimize_route():
    """
    Optimize route for given delivery points
    Uses mock optimization algorithm
    """
    try:
        data = request.json
        vehicle_id = data.get('vehicle_id')
        driver_id = data.get('driver_id')
        delivery_order_ids = data.get('delivery_points', [])
        
        logger.info(f"Optimizing route for vehicle {vehicle_id} with {len(delivery_order_ids)} points")
        
        # Get delivery points
        points = []
        for order_id in delivery_order_ids:
            if order_id in delivery_points:
                points.append({
                    'order_id': order_id,
                    'address': delivery_points[order_id]['address'],
                    'coordinates': delivery_points[order_id]['coordinates']
                })
        
        # Calculate optimized route
        route_data = calculate_route(points)
        
        route_id = f"ROUTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        routes[route_id] = {
            'route_id': route_id,
            'vehicle_id': vehicle_id,
            'driver_id': driver_id,
            'delivery_points': points,
            'total_distance_km': route_data['total_distance_km'],
            'estimated_time_minutes': route_data['estimated_time_minutes'],
            'waypoints': route_data['waypoints'],
            'optimization_score': route_data['optimization_score'],
            'status': 'active',
            'created_at': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Route optimized: {route_id}, distance: {route_data['total_distance_km']} km")
        
        return jsonify({
            'success': True,
            'route_id': route_id,
            'route_data': routes[route_id]
        })
    
    except Exception as e:
        logger.error(f"Error optimizing route: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/route/<route_id>', methods=['GET'])
def get_route(route_id):
    """Get route details"""
    if route_id in routes:
        return jsonify(routes[route_id])
    else:
        return jsonify({'error': 'Route not found'}), 404

@app.route('/route/<route_id>/update', methods=['PUT'])
def update_route(route_id):
    """Update route with new delivery point (high-priority)"""
    try:
        data = request.json
        new_order_id = data['order_id']
        
        logger.info(f"Updating route {route_id} with high-priority delivery {new_order_id}")
        
        if route_id in routes and new_order_id in delivery_points:
            # Re-optimize with new point
            routes[route_id]['delivery_points'].append({
                'order_id': new_order_id,
                'address': delivery_points[new_order_id]['address'],
                'coordinates': delivery_points[new_order_id]['coordinates'],
                'priority': 'high'
            })
            
            # Recalculate route
            route_data = calculate_route(routes[route_id]['delivery_points'])
            routes[route_id]['total_distance_km'] = route_data['total_distance_km']
            routes[route_id]['estimated_time_minutes'] = route_data['estimated_time_minutes']
            routes[route_id]['waypoints'] = route_data['waypoints']
            routes[route_id]['updated_at'] = datetime.utcnow().isoformat()
            
            return jsonify({
                'success': True,
                'route_id': route_id,
                'message': 'Route updated with new delivery point'
            })
        else:
            return jsonify({'success': False, 'error': 'Route or delivery point not found'}), 404
    
    except Exception as e:
        logger.error(f"Error updating route: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vehicles', methods=['GET'])
def list_vehicles():
    """List available vehicles"""
    return jsonify({'vehicles': vehicles})

@app.route('/vehicle/<vehicle_id>/status', methods=['PUT'])
def update_vehicle_status(vehicle_id):
    """Update vehicle status"""
    try:
        data = request.json
        status = data.get('status')
        
        if vehicle_id in vehicles:
            vehicles[vehicle_id]['status'] = status
            vehicles[vehicle_id]['updated_at'] = datetime.utcnow().isoformat()
            
            return jsonify({'success': True, 'vehicle_id': vehicle_id, 'status': status})
        else:
            return jsonify({'success': False, 'error': 'Vehicle not found'}), 404
    
    except Exception as e:
        logger.error(f"Error updating vehicle status: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/analytics/efficiency', methods=['GET'])
def get_route_efficiency():
    """Get route efficiency analytics"""
    total_routes = len(routes)
    total_distance = sum(r['total_distance_km'] for r in routes.values())
    avg_optimization_score = sum(r['optimization_score'] for r in routes.values()) / total_routes if total_routes > 0 else 0
    
    return jsonify({
        'total_routes': total_routes,
        'total_distance_km': round(total_distance, 2),
        'average_optimization_score': round(avg_optimization_score, 2),
        'active_routes': len([r for r in routes.values() if r['status'] == 'active'])
    })

if __name__ == '__main__':
    logger.info("Starting ROS Adapter (REST/JSON) on port 5003")
    app.run(host='0.0.0.0', port=5003, debug=True)
