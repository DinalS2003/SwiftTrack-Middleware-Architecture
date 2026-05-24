"""
Order Service - SwiftLogistics
Orchestrates order processing across CMS, ROS, and WMS
Implements Saga pattern for distributed transaction management
"""

from flask import Flask, request, jsonify
import requests
import pika
import json
import uuid
from datetime import datetime, timedelta
import logging
import threading
import time

from models import (
    Order, Package, Delivery, SagaState, PublishedEvent,
    SessionLocal, init_db, engine
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    init_db()
    logger.info("Database initialized successfully")
except Exception as e:
    logger.error(f"Database initialization failed: {str(e)}")

SERVICES = {
    'cms_adapter': 'http://cms-adapter:5002',
    'ros_adapter': 'http://ros-adapter:5003',
    'wms_adapter': 'http://wms-adapter:5004'
}

RABBITMQ_HOST = 'rabbitmq'

def get_rabbitmq_connection():
    """Create RabbitMQ connection"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        return connection
    except:
        logger.warning("RabbitMQ not available, running without message queue")
        return None

def publish_event(event_type, event_data):
    """Publish event to message queue and database"""
    try:
        connection = get_rabbitmq_connection()
        if connection:
            channel = connection.channel()
            channel.queue_declare(queue='order_events', durable=True)
            
            message = {
                'event_type': event_type,
                'data': event_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            channel.basic_publish(
                exchange='',
                routing_key='order_events',
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            connection.close()
            logger.info(f"Published event: {event_type}")
        
        db = SessionLocal()
        try:
            event = PublishedEvent(
                event_id=f"{event_type}-{uuid.uuid4().hex[:8]}",
                event_type=event_type,
                order_id=event_data.get('order_id'),
                delivery_id=event_data.get('delivery_id'),
                data=json.dumps(event_data)
            )
            db.add(event)
            db.commit()
            logger.info(f"Event saved to database: {event_type}")
        except Exception as db_error:
            logger.error(f"Failed to save event to database: {str(db_error)}")
            db.rollback()
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Failed to publish event: {str(e)}")

class OrderSaga:
    
    def __init__(self, order_id, order_data):
        self.order_id = order_id
        self.order_data = order_data
        self.steps_completed = []
        self.compensation_needed = False
    
    def execute(self):
        try:
            cms_result = self.register_in_cms()
            if not cms_result['success']:
                raise Exception("CMS registration failed")
            self.steps_completed.append('cms')
            
            wms_result = self.add_to_wms()
            if not wms_result['success']:
                raise Exception("WMS addition failed")
            self.steps_completed.append('wms')
            
            ros_result = self.add_to_ros()
            if not ros_result['success']:
                raise Exception("ROS addition failed")
            self.steps_completed.append('ros')
            
            logger.info(f"Saga completed successfully for order {self.order_id}")
            return {'success': True, 'order_id': self.order_id}
        
        except Exception as e:
            logger.error(f"Saga failed for order {self.order_id}: {str(e)}")
            self.compensate()
            return {'success': False, 'error': str(e)}
  
    def register_in_cms(self):
        """Register order in CMS (SOAP)"""
        try:
            response = requests.post(
                f"{SERVICES['cms_adapter']}/register_order",
                json={
                    'order_id': self.order_id,
                    'client_id': self.order_data['client_id'],
                    'delivery_address': self.order_data['delivery_address']
                },
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Order {self.order_id} registered in CMS")
                return {'success': True, 'data': response.json()}
            else:
                return {'success': False, 'error': 'CMS registration failed'}
        
        except Exception as e:
            logger.error(f"CMS error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def add_to_wms(self):
        """Add packages to WMS (TCP/IP)"""
        try:
            response = requests.post(
                f"{SERVICES['wms_adapter']}/add_packages",
                json={
                    'order_id': self.order_id,
                    'packages': self.order_data.get('packages', [])
                },
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Packages for order {self.order_id} added to WMS")
                return {'success': True, 'data': response.json()}
            else:
                return {'success': False, 'error': 'WMS addition failed'}
        
        except Exception as e:
            logger.error(f"WMS error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def add_to_ros(self):
        """Add delivery point to ROS (REST)"""
        try:
            response = requests.post(
                f"{SERVICES['ros_adapter']}/add_delivery_point",
                json={
                    'order_id': self.order_id,
                    'address': self.order_data['delivery_address'],
                    'priority': self.order_data.get('priority', 'normal')
                },
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Delivery point for order {self.order_id} added to ROS")
                return {'success': True, 'data': response.json()}
            else:
                return {'success': False, 'error': 'ROS addition failed'}
        
        except Exception as e:
            logger.error(f"ROS error: {str(e)}")
            return {'success': False, 'error': str(e)}
    # Compensation logic for saga
    def compensate(self):
        """Compensating transactions to rollback changes"""
        logger.info(f"Starting compensation for order {self.order_id}")
        
        if 'ros' in self.steps_completed:
            self.remove_from_ros()
        
        if 'wms' in self.steps_completed:
            self.remove_from_wms()
        
        if 'cms' in self.steps_completed:
            self.cancel_in_cms()
        
        logger.info(f"Compensation completed for order {self.order_id}")
    
    def remove_from_ros(self):
        """Compensate ROS addition"""
        try:
            requests.delete(
                f"{SERVICES['ros_adapter']}/delivery_point/{self.order_id}",
                timeout=5
            )
            logger.info(f"Removed order {self.order_id} from ROS")
        except Exception as e:
            logger.error(f"ROS compensation error: {str(e)}")
    
    def remove_from_wms(self):
        """Compensate WMS addition"""
        try:
            requests.delete(
                f"{SERVICES['wms_adapter']}/packages/{self.order_id}",
                timeout=5
            )
            logger.info(f"Removed packages for order {self.order_id} from WMS")
        except Exception as e:
            logger.error(f"WMS compensation error: {str(e)}")
    
    def cancel_in_cms(self):
        """Compensate CMS registration"""
        try:
            requests.delete(
                f"{SERVICES['cms_adapter']}/order/{self.order_id}",
                timeout=5
            )
            logger.info(f"Cancelled order {self.order_id} in CMS")
        except Exception as e:
            logger.error(f"CMS compensation error: {str(e)}")

# API Endpoints
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})
#Order creation endpoint with saga orchestration
@app.route('/orders', methods=['POST'])
def create_order():
    """Create new order and execute saga"""
    db = SessionLocal()
    try:
        order_data = request.json
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        
        # Normalize delivery_address field
        delivery_address = order_data.get('delivery_address') or order_data.get('address')
        if not delivery_address:
            return jsonify({'error': 'delivery_address or address is required'}), 400
        
        # Handle packages: convert integer count to list of package objects
        packages_count = order_data.get('packages', 1)
        
        # Store order in database
        order = Order(
            order_id=order_id,
            client_id=order_data['client_id'],
            recipient_name=order_data.get('recipient_name', 'Customer'),
            phone=order_data.get('phone', ''),
            delivery_address=delivery_address,
            address=delivery_address,
            city=order_data.get('city', ''),
            zip=order_data.get('zip', ''),
            priority=order_data.get('priority', 'normal'),
            notes=order_data.get('notes', ''),
            status='processing'
        )
        db.add(order)
        
        # Create package records
        if isinstance(packages_count, int):
            packages = []
            for i in range(packages_count):
                package = Package(
                    package_id=f"{order_id}-PKG{i+1}",
                    order_id=order_id,
                    weight=2.0,
                    dimensions='10x10x10cm'
                )
                db.add(package)
                packages.append({
                    'package_id': package.package_id,
                    'weight': 2.0,
                    'dimensions': '10x10x10cm'
                })
        else:
            packages = packages_count if isinstance(packages_count, list) else []
        
        db.commit()
        logger.info(f"Creating order {order_id} with {len(packages)} packages")
        
        # Prepare order data for saga
        saga_order_data = {
            'client_id': order_data['client_id'],
            'delivery_address': delivery_address,
            'packages': packages,
            'priority': order_data.get('priority', 'normal')
        }
        
        # Execute saga asynchronously
        def process_order_saga():
            saga = OrderSaga(order_id, saga_order_data)
            result = saga.execute()
            
            db_saga = SessionLocal()
            try:
                order_record = db_saga.query(Order).filter(Order.order_id == order_id).first()
                if order_record:
                    if result['success']:
                        order_record.status = 'order_confirmed'
                        order_record.confirmed_at = datetime.utcnow()
                        publish_event('order_confirmed', {'order_id': order_id})
                    else:
                        order_record.status = 'failed'
                        order_record.error = result.get('error')
                        publish_event('order_failed', {'order_id': order_id, 'error': result.get('error')})
                    db_saga.commit()
            finally:
                db_saga.close()
        
        # Start saga in background thread
        thread = threading.Thread(target=process_order_saga)
        thread.start()
        
        return jsonify({
            'order_id': order_id,
            'status': 'processing',
            'message': 'Order is being processed'
        }), 201
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating order: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/orders/<order_id>', methods=['GET'])
def get_order(order_id):
    """Get order details"""
    db = SessionLocal()
    try:
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if order:
            order_dict = order.to_dict()
            # Count packages
            package_count = db.query(Package).filter(Package.order_id == order_id).count()
            order_dict['packages'] = package_count
            return jsonify(order_dict)
        else:
            return jsonify({'error': 'Order not found'}), 404
    except Exception as e:
        logger.error(f"Error fetching order: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/orders', methods=['GET'])
def list_orders():
    """List orders for a client"""
    db = SessionLocal()
    try:
        client_id = request.args.get('client_id')
        
        if client_id:
            orders_query = db.query(Order).filter(Order.client_id == client_id).all()
        else:
            orders_query = db.query(Order).all()
        
        # Convert to dict and include package count
        orders_list = []
        for order in orders_query:
            order_dict = order.to_dict()
            # Count packages for this order
            package_count = db.query(Package).filter(Package.order_id == order.order_id).count()
            order_dict['packages'] = package_count
            orders_list.append(order_dict)
        
        return jsonify({'orders': orders_list})
    except Exception as e:
        logger.error(f"Error listing orders: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/driver/manifest', methods=['GET'])
def get_driver_manifest():
    """Get driver's delivery manifest"""
    db = SessionLocal()
    try:
        driver_id = request.args.get('driver_id')
        date = request.args.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
        
        # Get orders that drivers can act on
        orders_query = db.query(Order).filter(
            Order.status.in_(['processing', 'order_confirmed', 'dispatched_from_warehouse', 'delivered', 'failed'])
        ).all()
        
        manifest_deliveries = []
        
        for order in orders_query:
            delivery_id = f"DEL-{order.order_id.split('-')[1]}"
            
            # Use order status for driver workflow
            delivery_status = order.status
            
            # Count packages
            package_count = db.query(Package).filter(Package.order_id == order.order_id).count()
            
            manifest_deliveries.append({
                'delivery_id': delivery_id,
                'order_id': order.order_id,
                'client_id': order.client_id,
                'address': order.delivery_address,
                'status': delivery_status,
                'packages': package_count,
                'priority': order.priority,
                'notes': order.notes if order.notes else ''
            })
        
        # Calculate route statistics
        total_deliveries = len(manifest_deliveries)
        completed = len([d for d in manifest_deliveries if d['status'] == 'delivered'])
        failed = len([d for d in manifest_deliveries if d['status'] == 'failed'])
        pending = len([d for d in manifest_deliveries if d['status'] in ['processing', 'order_confirmed', 'dispatched_from_warehouse']])
        
        manifest = {
            'driver_id': driver_id,
            'date': date,
            'deliveries': manifest_deliveries,
            'route': {
                'total_distance': f'{total_deliveries * 8.5:.1f} km',
                'estimated_time': f'{total_deliveries * 35}m',
                'stops': total_deliveries,
                'completed': completed,
                'failed': failed,
                'pending': pending
            }
        }
        
        return jsonify(manifest)
    except Exception as e:
        logger.error(f"Error getting driver manifest: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()
#Delivery status update endpoint for drivers
@app.route('/delivery/<delivery_id>', methods=['PUT'])
def update_delivery_status(delivery_id):
    """Update delivery status"""
    db = SessionLocal()
    try:
        update_data = request.json
        status = update_data.get('status')
        driver_id = update_data.get('driver_id')
        
        # Find the order associated with this delivery
        order_id = f"ORD-{delivery_id.split('-')[1]}"
        
        # Get order from database
        order = db.query(Order).filter(Order.order_id == order_id).first()
        client_id = order.client_id if order else None
        
        # Update order status based on delivery status
        if order:
            if status == 'delivered':
                order.status = 'completed'
            elif status == 'failed':
                order.status = 'failed'
        
        # Create or update delivery record
        delivery =db.query(Delivery).filter(Delivery.delivery_id == delivery_id).first()
        if not delivery:
            delivery = Delivery(
                delivery_id=delivery_id,
                order_id=order_id,
                client_id=client_id
            )
            db.add(delivery)
        
        delivery.status = status
        delivery.driver_id = driver_id
        delivery.notes = update_data.get('notes', '')
        delivery.updated_at = datetime.utcnow()
        
        db.commit()
        logger.info(f"Delivery {delivery_id} updated to {status} by driver {driver_id}")
        
        # Publish event
        publish_event('delivery_updated', {
            'delivery_id': delivery_id,
            'order_id': order_id,
            'status': status,
            'driver_id': driver_id,
            'client_id': client_id,
            'notes': update_data.get('notes', '')
        })
        
        return jsonify({
            'delivery_id': delivery_id,
            'order_id': order_id,
            'status': status,
            'client_id': client_id,
            'updated_at': delivery.updated_at.isoformat()
        })
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating delivery: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

# NEW: Delivery Dashboard Endpoints
@app.route('/orders/<order_id>/tracking', methods=['GET'])
def get_order_tracking(order_id):
    """Get detailed order tracking information for dashboard"""
    db = SessionLocal()
    try:
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if not order:
            return jsonify({'error': 'Order not found'}), 404
        
        # Get package count
        package_count = db.query(Package).filter(Package.order_id == order_id).count()
        
        # Get delivery info
        delivery_id = f"DEL-{order_id.split('-')[1]}"
        delivery = db.query(Delivery).filter(Delivery.delivery_id == delivery_id).first()
        
        # Calculate progress
        status_progress = {
            'processing': 0,
            'dispatched_from_warehouse': 60,
            'delivered': 100,
            'failed': 100
        }
        current_progress = status_progress.get(order.status, 0)
        
        # Status timeline
        timeline = [
            {
                'status': 'processing',
                'label': 'Processing',
                'completed': order.status in ['processing', 'dispatched_from_warehouse', 'delivered'],
                'timestamp': order.processing_at.isoformat() if order.processing_at else None,
                'icon': '📋'
            },
            {
                'status': 'dispatched_from_warehouse',
                'label': 'Dispatched from Warehouse',
                'completed': order.status in ['dispatched_from_warehouse', 'delivered'],
                'timestamp': order.dispatched_at.isoformat() if order.dispatched_at else None,
                'icon': '🚚'
            },
            {
                'status': 'delivered',
                'label': 'Delivered',
                'completed': order.status == 'delivered',
                'timestamp': order.delivered_at.isoformat() if order.delivered_at else None,
                'icon': '📦'
            }
        ]
        
        # Add failed status if order failed
        if order.status == 'failed':
            timeline.append({
                'status': 'failed',
                'label': 'Delivery Failed',
                'completed': True,
                'timestamp': order.failed_at.isoformat() if order.failed_at else None,
                'icon': '❌'
            })
        
        tracking_info = {
            'order_id': order_id,
            'status': order.status,
            'progress_percentage': current_progress,
            'client_id': order.client_id,
            'recipient_name': order.recipient_name,
            'delivery_address': order.delivery_address,
            'phone': order.phone,
            'packages': package_count,
            'priority': order.priority,
            'notes': order.notes,
            'driver_id': order.driver_id,
            'driver_name': order.driver_name,
            'current_location': order.current_location,
            'estimated_delivery': order.estimated_delivery.isoformat() if order.estimated_delivery else None,
            'timeline': timeline,
            'created_at': order.created_at.isoformat(),
            'updated_at': order.updated_at.isoformat()
        }
        
        return jsonify(tracking_info)
    except Exception as e:
        logger.error(f"Error getting tracking info: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/orders/<order_id>/status', methods=['PUT'])
def update_order_status(order_id):
    """Update order status and track timestamps"""
    db = SessionLocal()
    try:
        data = request.json
        new_status = data.get('status')  # processing, dispatched_from_warehouse, delivered
        
        if new_status not in ['processing', 'dispatched_from_warehouse', 'delivered', 'failed']:
            return jsonify({'error': 'Invalid status'}), 400
        
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if not order:
            return jsonify({'error': 'Order not found'}), 404
        
        old_status = order.status
        order.status = new_status
        
        # Update status timestamps
        now = datetime.utcnow()
        if new_status == 'processing' and not order.processing_at:
            order.processing_at = now
        elif new_status == 'dispatched_from_warehouse':
            order.dispatched_at = now
            # Set estimated delivery time (assumed 2-4 hours from dispatch)
            if not order.estimated_delivery:
                order.estimated_delivery = now + timedelta(hours=3)
        elif new_status == 'delivered':
            order.delivered_at = now
        elif new_status == 'failed':
            order.failed_at = now
        
        # Update driver info if provided
        if 'driver_id' in data:
            order.driver_id = data['driver_id']
        if 'driver_name' in data:
            order.driver_name = data['driver_name']
        if 'current_location' in data:
            order.current_location = data['current_location']
        
        order.updated_at = now
        db.commit()
        
        logger.info(f"Order {order_id} status updated from {old_status} to {new_status}")
        
        # Publish event
        publish_event('order_status_updated', {
            'order_id': order_id,
            'old_status': old_status,
            'new_status': new_status,
            'driver_id': order.driver_id,
            'timestamp': now.isoformat()
        })
        
        return jsonify({
            'order_id': order_id,
            'client_id': order.client_id,
            'old_status': old_status,
            'new_status': new_status,
            'timestamp': now.isoformat()
        })
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating order status: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/orders/<order_id>/location', methods=['PUT'])
def update_order_location(order_id):
    """Update driver location for real-time tracking"""
    db = SessionLocal()
    try:
        data = request.json
        current_location = data.get('location')  # lat,lng or address
        
        order = db.query(Order).filter(Order.order_id == order_id).first()
        if not order:
            return jsonify({'error': 'Order not found'}), 404
        
        order.current_location = current_location
        order.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Order {order_id} location updated to {current_location}")
        
        # Publish event for real-time updates
        publish_event('order_location_updated', {
            'order_id': order_id,
            'location': current_location,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return jsonify({
            'order_id': order_id,
            'location': current_location
        })
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating order location: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/dashboard/orders', methods=['GET'])
def get_dashboard_orders():
    """Get all orders for dashboard with filtering"""
    db = SessionLocal()
    try:
        client_id = request.args.get('client_id')
        status_filter = request.args.get('status')
        
        query = db.query(Order)
        
        if client_id:
            query = query.filter(Order.client_id == client_id)
        
        if status_filter:
            query = query.filter(Order.status == status_filter)
        
        orders_query = query.order_by(Order.created_at.desc()).all()
        
        # Convert to dict and include package count
        orders_list = []
        for order in orders_query:
            order_dict = order.to_dict()
            package_count = db.query(Package).filter(Package.order_id == order.order_id).count()
            order_dict['packages'] = package_count
            
            # Add progress
            status_progress = {
                'processing': 0,
                'dispatched_from_warehouse': 60,
                'delivered': 100,
                'failed': 100
            }
            order_dict['progress_percentage'] = status_progress.get(order.status, 0)
            orders_list.append(order_dict)
        
        return jsonify({'orders': orders_list})
    except Exception as e:
        logger.error(f"Error fetching dashboard orders: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/events', methods=['GET'])
def get_published_events():
    """Get recently published RabbitMQ events for monitoring"""
    db = SessionLocal()
    try:
        # Get last 20 published events from database
        events_query = db.query(PublishedEvent).order_by(PublishedEvent.published_at.desc()).limit(20).all()
        recent_events = [event.to_dict() for event in reversed(events_query)]
        
        # Get total count
        total_count = db.query(PublishedEvent).count()
        
        return jsonify({
            'total_events_published': total_count,
            'recent_events': recent_events,
            'message': 'Events are asynchronously published to RabbitMQ and persisted to database'
        })
    except Exception as e:
        logger.error(f"Error fetching events: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

if __name__ == '__main__':
    logger.info("Starting Order Service on port 5001")
    app.run(host='0.0.0.0', port=5001, debug=True)
