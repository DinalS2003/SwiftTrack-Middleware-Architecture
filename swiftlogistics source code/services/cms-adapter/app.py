"""
CMS Adapter - SwiftLogistics
Adapter for legacy Client Management System (SOAP/XML)
Translates REST requests to SOAP calls
"""

from flask import Flask, request, jsonify
import xmltodict
import xml.etree.ElementTree as ET
from datetime import datetime
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock CMS database
cms_orders = {}
cms_clients = {
    'CLIENT001': {'name': 'E-Commerce Ltd', 'contract_id': 'CTR001', 'billing_plan': 'premium'},
    'CLIENT002': {'name': 'Online Store Inc', 'contract_id': 'CTR002', 'billing_plan': 'standard'}
}

def create_soap_envelope(body_content):
    """Create SOAP envelope"""
    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:cms="http://swiftlogistics.com/cms">
    <soap:Header/>
    <soap:Body>
        {body_content}
    </soap:Body>
</soap:Envelope>"""
    return envelope

def parse_soap_response(xml_string):
    """Parse SOAP response"""
    try:
        data = xmltodict.parse(xml_string)
        return data
    except Exception as e:
        logger.error(f"Error parsing SOAP response: {str(e)}")
        return None

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'cms-adapter'})

@app.route('/register_order', methods=['POST'])
def register_order():
    """
    Register order in CMS (SOAP call simulation)
    Converts REST/JSON to SOAP/XML
    """
    try:
        data = request.json
        order_id = data['order_id']
        client_id = data['client_id']
        delivery_address = data['delivery_address']
        
        logger.info(f"Registering order {order_id} for client {client_id} in CMS")
        
        # Create SOAP request body
        soap_body = f"""
        <cms:RegisterOrder>
            <cms:OrderID>{order_id}</cms:OrderID>
            <cms:ClientID>{client_id}</cms:ClientID>
            <cms:DeliveryAddress>{delivery_address}</cms:DeliveryAddress>
            <cms:Timestamp>{datetime.utcnow().isoformat()}</cms:Timestamp>
        </cms:RegisterOrder>
        """
        
        soap_request = create_soap_envelope(soap_body)
        
        # Simulate SOAP call to legacy CMS
        # In production, use suds-jurko or zeep library
        logger.info(f"SOAP Request:\n{soap_request}")
        
        # Mock CMS response
        cms_orders[order_id] = {
            'order_id': order_id,
            'client_id': client_id,
            'delivery_address': delivery_address,
            'contract_id': cms_clients.get(client_id, {}).get('contract_id'),
            'status': 'registered',
            'registered_at': datetime.utcnow().isoformat()
        }
        
        # Create SOAP response
        soap_response_body = f"""
        <cms:RegisterOrderResponse>
            <cms:OrderID>{order_id}</cms:OrderID>
            <cms:Status>SUCCESS</cms:Status>
            <cms:Message>Order registered successfully</cms:Message>
            <cms:ContractID>{cms_clients.get(client_id, {}).get('contract_id', 'N/A')}</cms:ContractID>
        </cms:RegisterOrderResponse>
        """
        
        soap_response = create_soap_envelope(soap_response_body)
        logger.info(f"SOAP Response:\n{soap_response}")
        
        # Return REST/JSON response
        return jsonify({
            'success': True,
            'order_id': order_id,
            'status': 'registered',
            'contract_id': cms_clients.get(client_id, {}).get('contract_id')
        })
    
    except Exception as e:
        logger.error(f"Error registering order in CMS: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/order/<order_id>', methods=['GET'])
def get_order(order_id):
    """Get order from CMS"""
    if order_id in cms_orders:
        return jsonify(cms_orders[order_id])
    else:
        return jsonify({'error': 'Order not found in CMS'}), 404

@app.route('/order/<order_id>', methods=['DELETE'])
def cancel_order(order_id):
    """Cancel order in CMS (compensation transaction)"""
    try:
        logger.info(f"Cancelling order {order_id} in CMS")
        
        if order_id in cms_orders:
            cms_orders[order_id]['status'] = 'cancelled'
            cms_orders[order_id]['cancelled_at'] = datetime.utcnow().isoformat()
            
            # SOAP cancellation request
            soap_body = f"""
            <cms:CancelOrder>
                <cms:OrderID>{order_id}</cms:OrderID>
                <cms:Reason>Saga compensation</cms:Reason>
            </cms:CancelOrder>
            """
            
            soap_request = create_soap_envelope(soap_body)
            logger.info(f"SOAP Cancellation Request:\n{soap_request}")
            
            return jsonify({'success': True, 'order_id': order_id, 'status': 'cancelled'})
        else:
            return jsonify({'success': False, 'error': 'Order not found'}), 404
    
    except Exception as e:
        logger.error(f"Error cancelling order in CMS: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/client/<client_id>', methods=['GET'])
def get_client(client_id):
    """Get client information from CMS"""
    if client_id in cms_clients:
        return jsonify(cms_clients[client_id])
    else:
        return jsonify({'error': 'Client not found'}), 404

@app.route('/billing/invoice', methods=['POST'])
def create_invoice():
    """Create billing invoice (SOAP call)"""
    try:
        data = request.json
        order_id = data['order_id']
        
        # SOAP request for invoice creation
        soap_body = f"""
        <cms:CreateInvoice>
            <cms:OrderID>{order_id}</cms:OrderID>
            <cms:InvoiceDate>{datetime.utcnow().isoformat()}</cms:InvoiceDate>
        </cms:CreateInvoice>
        """
        
        soap_request = create_soap_envelope(soap_body)
        logger.info(f"Creating invoice for order {order_id}")
        
        return jsonify({
            'success': True,
            'invoice_id': f"INV-{order_id}",
            'created_at': datetime.utcnow().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error creating invoice: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting CMS Adapter (SOAP/XML) on port 5002")
    app.run(host='0.0.0.0', port=5002, debug=True)
