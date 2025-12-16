# Archivo: server/simple_soap_server.py
# SERVIDOR SOAP CON PROCESAMIENTO PARALELO Y LOAD BALANCING

import os
import sys
import tempfile
import base64
import json
import time
import gzip
from http.server import HTTPServer, BaseHTTPRequestHandler
from lxml import etree
import xml.etree.ElementTree as ET
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Importar cliente REST
from rest_client import RestClient

# Importar gestor de sesiones
from session_manager import SessionManager

# Importar load balancer
from load_balancer import LoadBalancer

# IMPORTACIÓN DEL CLIENTE gRPC
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    
    from grpc_client.client import NodeClient
    print("[SERVIDOR] ✓ NodeClient importado correctamente")
    
except ImportError as e:
    print(f"[SERVIDOR] ✗ Error al importar NodeClient:")
    print(f"    Error: {e}")
    sys.exit(1)

# Crear instancia del cliente REST
rest_client = RestClient('http://localhost:5000')

# Crear gestor de sesiones global
session_manager = SessionManager(expiration_hours=24)

# Crear load balancer
load_balancer = LoadBalancer(rest_client)

# Pool de threads para procesamiento paralelo
thread_pool = ThreadPoolExecutor(max_workers=10)

# WSDL Template
WSDL_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<definitions name="ImageProcessingService"
             targetNamespace="http://example.org/ImageProcessingService.wsdl"
             xmlns="http://schemas.xmlsoap.org/wsdl/"
             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
             xmlns:tns="http://example.org/ImageProcessingService.wsdl"
             xmlns:xsd="http://www.w3.org/2001/XMLSchema">

    <types>
        <xsd:schema targetNamespace="http://example.org/ImageProcessingService.wsdl">
            
            <!-- REGISTRO -->
            <xsd:element name="RegisterRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="username" type="xsd:string"/>
                        <xsd:element name="password" type="xsd:string"/>
                        <xsd:element name="email" type="xsd:string"/>
                        <xsd:element name="first_name" type="xsd:string" minOccurs="0"/>
                        <xsd:element name="last_name" type="xsd:string" minOccurs="0"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="RegisterResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="message" type="xsd:string"/>
                        <xsd:element name="user_id" type="xsd:int"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- LOGIN -->
            <xsd:element name="LoginRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="username" type="xsd:string"/>
                        <xsd:element name="password" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="LoginResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="message" type="xsd:string"/>
                        <xsd:element name="session_token" type="xsd:string"/>
                        <xsd:element name="user_id" type="xsd:int"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- LOGOUT -->
            <xsd:element name="LogoutRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="session_token" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="LogoutResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="message" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- PROCESAR LOTE -->
            <xsd:element name="ProcessBatchRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="session_token" type="xsd:string"/>
                        <xsd:element name="batch_name" type="xsd:string"/>
                        <xsd:element name="images_json" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="ProcessBatchResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="message" type="xsd:string"/>
                        <xsd:element name="batch_id" type="xsd:int"/>
                        <xsd:element name="total_images" type="xsd:int"/>
                        <xsd:element name="processed_images" type="xsd:int"/>
                        <xsd:element name="failed_images" type="xsd:int"/>
                        <xsd:element name="processing_time_ms" type="xsd:int"/>
                        <xsd:element name="download_url" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- MÉTRICAS DE NODOS -->
            <xsd:element name="GetNodesMetricsRequest">
                <xsd:complexType>
                    <xsd:sequence/>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="GetNodesMetricsResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="nodes_json" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- MÉTRICAS DE LOTE -->
            <xsd:element name="GetBatchMetricsRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="batch_id" type="xsd:int"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="GetBatchMetricsResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="metrics_json" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            
            <!-- NodeHeartbeat -->
            <xsd:element name="NodeHeartbeatRequest">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="node_id" type="xsd:int"/>
                        <xsd:element name="ip_address" type="xsd:string"/>
                        <xsd:element name="port" type="xsd:int"/>
                        <xsd:element name="cpu_cores" type="xsd:int"/>
                        <xsd:element name="ram_gb" type="xsd:float"/>
                        <xsd:element name="current_load" type="xsd:int"/>
                        <xsd:element name="status" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
            <xsd:element name="NodeHeartbeatResponse">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name="success" type="xsd:boolean"/>
                        <xsd:element name="message" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
        </xsd:schema>
    </types>

    <message name="RegisterInput">
        <part name="parameters" element="tns:RegisterRequest"/>
    </message>
    <message name="RegisterOutput">
        <part name="parameters" element="tns:RegisterResponse"/>
    </message>
    <message name="LoginInput">
        <part name="parameters" element="tns:LoginRequest"/>
    </message>
    <message name="LoginOutput">
        <part name="parameters" element="tns:LoginResponse"/>
    </message>
    <message name="LogoutInput">
        <part name="parameters" element="tns:LogoutRequest"/>
    </message>
    <message name="LogoutOutput">
        <part name="parameters" element="tns:LogoutResponse"/>
    </message>
    <message name="ProcessBatchInput">
        <part name="parameters" element="tns:ProcessBatchRequest"/>
    </message>
    <message name="ProcessBatchOutput">
        <part name="parameters" element="tns:ProcessBatchResponse"/>
    </message>
    <message name="GetNodesMetricsInput">
        <part name="parameters" element="tns:GetNodesMetricsRequest"/>
    </message>
    <message name="GetNodesMetricsOutput">
        <part name="parameters" element="tns:GetNodesMetricsResponse"/>
    </message>
    <message name="GetBatchMetricsInput">
        <part name="parameters" element="tns:GetBatchMetricsRequest"/>
    </message>
    <message name="GetBatchMetricsOutput">
        <part name="parameters" element="tns:GetBatchMetricsResponse"/>
    </message>
    <message name="NodeHeartbeatInput">
        <part name="parameters" element="tns:NodeHeartbeatRequest"/>
    </message>
    <message name="NodeHeartbeatOutput">
        <part name="parameters" element="tns:NodeHeartbeatResponse"/>
    </message>

    <portType name="ImageProcessingPortType">
        <operation name="Register">
            <input message="tns:RegisterInput"/>
            <output message="tns:RegisterOutput"/>
        </operation>
        <operation name="Login">
            <input message="tns:LoginInput"/>
            <output message="tns:LoginOutput"/>
        </operation>
        <operation name="Logout">
            <input message="tns:LogoutInput"/>
            <output message="tns:LogoutOutput"/>
        </operation>
        <operation name="ProcessBatch">
            <input message="tns:ProcessBatchInput"/>
            <output message="tns:ProcessBatchOutput"/>
        </operation>
        <operation name="GetNodesMetrics">
            <input message="tns:GetNodesMetricsInput"/>
            <output message="tns:GetNodesMetricsOutput"/>
        </operation>
        <operation name="GetBatchMetrics">
            <input message="tns:GetBatchMetricsInput"/>
            <output message="tns:GetBatchMetricsOutput"/>
        </operation>
        <operation name="NodeHeartbeat">
            <input message="tns:NodeHeartbeatInput"/>
            <output message="tns:NodeHeartbeatOutput"/>
        </operation>
    </portType>

    <binding name="ImageProcessingBinding" type="tns:ImageProcessingPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="Register">
            <soap:operation soapAction="Register"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="Login">
            <soap:operation soapAction="Login"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="Logout">
            <soap:operation soapAction="Logout"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="ProcessBatch">
            <soap:operation soapAction="ProcessBatch"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="GetNodesMetrics">
            <soap:operation soapAction="GetNodesMetrics"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="GetBatchMetrics">
            <soap:operation soapAction="GetBatchMetrics"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
        <operation name="NodeHeartbeat">
            <soap:operation soapAction="NodeHeartbeat"/>
            <input><soap:body use="literal"/></input>
            <output><soap:body use="literal"/></output>
        </operation>
    </binding>

    <service name="ImageProcessingService">
        <port name="ImageProcessingPort" binding="tns:ImageProcessingBinding">
            <soap:address location="http://localhost:8000/soap"/>
        </port>
    </service>
</definitions>
'''

class SOAPHandler(BaseHTTPRequestHandler):
    """MANEJADOR HTTP PARA SOLICITUDES SOAP"""

    def __init__(self, *args, **kwargs):
        self.rfile_max_size = 50 * 1024 * 1024  # 50 MB
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """MANEJO DE SOLICITUDES GET"""
        if self.path == '/wsdl' or self.path == '/?wsdl':
            self.send_response(200)
            self.send_header('Content-type', 'text/xml')
            self.end_headers()
            self.wfile.write(WSDL_TEMPLATE.encode())
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html_response = '''
            <html>
            <head>
                <title>Servicio SOAP de Procesamiento de Imagenes</title>
            </head>
            <body>
                <h1>Servicio SOAP de Procesamiento de Imagenes</h1>
                <p>WSDL disponible en: <a href="/wsdl">/wsdl</a></p>
                <p>Este servicio acepta solicitudes SOAP en el endpoint: /soap</p>
                <h2>Operaciones Disponibles:</h2>
                <ul>
                    <li><strong>Register</strong>: Registrar nuevo usuario</li>
                    <li><strong>Login</strong>: Iniciar sesion</li>
                    <li><strong>Logout</strong>: Cerrar sesion</li>
                    <li><strong>ProcessBatch</strong>: Procesar lote de imagenes (requiere sesion)</li>
                    <li><strong>GetNodesMetrics</strong>: Obtener metricas de nodos</li>
                    <li><strong>GetBatchMetrics</strong>: Obtener metricas de un lote</li>
                </ul>
                <h2>Características:</h2>
                <ul>
                    <li>✓ Procesamiento paralelo con threads</li>
                    <li>✓ Load balancing por peso de imágenes</li>
                    <li>✓ Sistema de logs completo</li>
                    <li>✓ Distribución equitativa entre nodos</li>
                    <li>✓ Descarga de resultados en ZIP</li>
                    <li>✓ Métricas de consumo</li>
                </ul>
            </body>
            </html>
            '''
            self.wfile.write(html_response.encode('utf-8'))
    
    def do_POST(self):
        """MANEJO DE SOLICITUDES POST (SOAP)"""
        if self.path == '/soap':
            content_length = int(self.headers['Content-Length'])
            
            max_size = 50 * 1024 * 1024
            if content_length > max_size:
                self.send_error(413, 'Request demasiado grande')
                return
            
            post_data = self.rfile.read(content_length)
            
            try:
                root = ET.fromstring(post_data)
                namespaces = {
                    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'ns': 'http://example.org/ImageProcessingService.wsdl'
                }
                
                body = root.find('.//soap:Body', namespaces)
                
                register_request = body.find('.//ns:RegisterRequest', namespaces)
                login_request = body.find('.//ns:LoginRequest', namespaces)
                logout_request = body.find('.//ns:LogoutRequest', namespaces)
                process_batch_request = body.find('.//ns:ProcessBatchRequest', namespaces)
                metrics_nodes_request = body.find('.//ns:GetNodesMetricsRequest', namespaces)
                metrics_batch_request = body.find('.//ns:GetBatchMetricsRequest', namespaces)
                
                result = None
                response_type = None
                
                if register_request is not None:
                    result = self.handle_register(register_request, namespaces)
                    response_type = 'Register'
                    
                elif login_request is not None:
                    result = self.handle_login(login_request, namespaces)
                    response_type = 'Login'
                    
                elif logout_request is not None:
                    result = self.handle_logout(logout_request, namespaces)
                    response_type = 'Logout'
                    
                elif process_batch_request is not None:
                    session_token = process_batch_request.find('.//ns:session_token', namespaces).text
                    batch_name = process_batch_request.find('.//ns:batch_name', namespaces).text
                    images_json = process_batch_request.find('.//ns:images_json', namespaces).text
                    
                    result = self.process_batch(session_token, batch_name, images_json)
                    response_type = 'ProcessBatch'
                
                elif metrics_nodes_request is not None:
                    result = self.handle_get_nodes_metrics()
                    response_type = 'GetNodesMetrics'
                
                elif metrics_batch_request is not None:
                    batch_id = int(metrics_batch_request.find('.//ns:batch_id', namespaces).text)
                    result = self.handle_get_batch_metrics(batch_id)
                    response_type = 'GetBatchMetrics'
                
                # ⭐ NUEVO: NodeHeartbeat
                heartbeat_request = body.find('.//ns:NodeHeartbeatRequest', namespaces)
                if heartbeat_request is not None:
                    node_id = int(heartbeat_request.find('.//ns:node_id', namespaces).text)
                    ip_address = heartbeat_request.find('.//ns:ip_address', namespaces).text
                    port = int(heartbeat_request.find('.//ns:port', namespaces).text)
                    cpu_cores = int(heartbeat_request.find('.//ns:cpu_cores', namespaces).text)
                    ram_gb = float(heartbeat_request.find('.//ns:ram_gb', namespaces).text)
                    current_load = int(heartbeat_request.find('.//ns:current_load', namespaces).text)
                    status = heartbeat_request.find('.//ns:status', namespaces).text
                    
                    result = self.handle_node_heartbeat(node_id, ip_address, port, cpu_cores, ram_gb, current_load, status)
                    response_type = 'NodeHeartbeat'
                
                if result and response_type:
                    soap_response = self.create_soap_response(response_type, result)
                    self.send_response(200)
                    self.send_header('Content-type', 'text/xml')
                    self.end_headers()
                    self.wfile.write(soap_response.encode())
                else:
                    self.send_error(400, 'Invalid SOAP request')
                    
            except Exception as e:
                print(f"Error procesando solicitud SOAP: {e}")
                import traceback
                traceback.print_exc()
                self.send_error(500, f'Internal Server Error: {str(e)}')
        else:
            self.send_error(404, 'Service not found')
    
    def handle_register(self, request, namespaces):
        """Manejar registro de usuario"""
        username = request.find('.//ns:username', namespaces).text
        password = request.find('.//ns:password', namespaces).text
        email = request.find('.//ns:email', namespaces).text
        first_name_elem = request.find('.//ns:first_name', namespaces)
        last_name_elem = request.find('.//ns:last_name', namespaces)
        
        first_name = first_name_elem.text if first_name_elem is not None else None
        last_name = last_name_elem.text if last_name_elem is not None else None
        
        print(f"[SERVIDOR] Registro: username={username}, email={email}")
        
        success, result = rest_client.register_user(
            username=username,
            password=password,
            email=email,
            first_name=first_name,
            last_name=last_name
        )
        
        if success:
            return {
                'success': True,
                'message': result.get('message', 'Usuario registrado'),
                'user_id': result.get('user_id', 0)
            }
        else:
            return {
                'success': False,
                'message': result.get('error', 'Error al registrar'),
                'user_id': 0
            }
    
    def handle_login(self, request, namespaces):
        """Manejar login de usuario"""
        username = request.find('.//ns:username', namespaces).text
        password = request.find('.//ns:password', namespaces).text
        
        print(f"[SERVIDOR] Login: username={username}")
        
        success, result = rest_client.login_user(username, password)
        
        if success:
            session_token = session_manager.create_session(
                user_id=result['user_id'],
                username=result['username']
            )
            
            return {
                'success': True,
                'message': 'Login exitoso',
                'session_token': session_token,
                'user_id': result['user_id']
            }
        else:
            return {
                'success': False,
                'message': result.get('error', 'Credenciales inválidas'),
                'session_token': '',
                'user_id': 0
            }
    
    def handle_logout(self, request, namespaces):
        """Manejar logout de usuario"""
        session_token = request.find('.//ns:session_token', namespaces).text
        
        print(f"[SERVIDOR] Logout: token={session_token[:8]}...")
        
        destroyed = session_manager.destroy_session(session_token)
        
        return {
            'success': destroyed,
            'message': 'Logout exitoso' if destroyed else 'Sesión no encontrada'
        }
    
    def handle_get_nodes_metrics(self):
        """Obtener métricas de nodos"""
        print(f"[SERVIDOR] GetNodesMetrics")
        
        try:
            # Consultar DB Service
            success, nodes = rest_client.get_active_nodes()
            
            if success:
                print(f"[SERVIDOR] Métricas obtenidas: {len(nodes)} nodos")
                return {
                    'success': True,
                    'nodes_json': json.dumps(nodes)
                }
            else:
                print(f"[SERVIDOR] Error obteniendo métricas")
                return {
                    'success': False,
                    'nodes_json': '[]'
                }
        except Exception as e:
            print(f"[SERVIDOR] Error en GetNodesMetrics: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'nodes_json': '[]'
            }
    
    def handle_get_batch_metrics(self, batch_id):
        """Obtener métricas de lote"""
        print(f"[SERVIDOR] GetBatchMetrics: batch_id={batch_id}")
        
        try:
            # Consultar batch
            success, batch_data = rest_client.get_batch(batch_id)
            if not success:
                print(f"[SERVIDOR] Lote {batch_id} no encontrado")
                return {
                    'success': False,
                    'metrics_json': '{}'
                }
            
            # Construir métricas básicas
            total_images = batch_data.get('total_images', 0)
            processed_images = batch_data.get('processed_images', 0)
            
            metrics = {
                'batch_id': batch_id,
                'batch_name': batch_data.get('batch_name', ''),
                'total_images': total_images,
                'processed_images': processed_images,
                'failed_images': total_images - processed_images,
                'status': batch_data.get('status', ''),
                'created_at': str(batch_data.get('created_at', ''))
            }
            
            print(f"[SERVIDOR] Métricas de lote {batch_id} obtenidas")
            return {
                'success': True,
                'metrics_json': json.dumps(metrics)
            }
        except Exception as e:
            print(f"[SERVIDOR] Error en GetBatchMetrics: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'metrics_json': '{}'
            }
    
    #Registra/actualiza el nodo en la base de datos

    def handle_node_heartbeat(self, node_id, ip_address, port, cpu_cores, ram_gb, current_load, status):
        """Recibir heartbeat de nodo y propagarlo al DB Service"""
        print(f"[SERVIDOR] NodeHeartbeat recibido: node_id={node_id}, port={port}")
        
        try:
            # Propagar al DB Service
            success, result = rest_client.update_node_heartbeat(
                node_id=node_id,
                ip_address=ip_address,
                port=port,
                cpu_cores=cpu_cores,
                ram_gb=ram_gb,
                current_load=current_load,
                status=status
            )
            
            if success:
                print(f"[SERVIDOR] Heartbeat de nodo {node_id} registrado en DB")
                return {
                    'success': True,
                    'message': 'Heartbeat registrado'
                }
            else:
                print(f"[SERVIDOR] Error registrando heartbeat: {result}")
                return {
                    'success': False,
                    'message': 'Error al registrar heartbeat'
                }
        except Exception as e:
            print(f"[SERVIDOR] Error en NodeHeartbeat: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'metrics_json': '{}'
            }
    
    def process_batch(self, session_token, batch_name, images_json):
        """PROCESAR LOTE DE IMÁGENES EN PARALELO CON DISTRIBUCIÓN POR PESO"""
        start_time = time.time()
        
        try:
 #########################################################################################################################################           # VALIDAR SESIÓN
            session = session_manager.validate_session(session_token)
            if not session:
                return {
                    'success': False,
                    'message': 'Sesión inválida o expirada',
                    'batch_id': 0,
                    'total_images': 0,
                    'processed_images': 0,
                    'failed_images': 0,
                    'processing_time_ms': 0,
                    'download_url': ''
                }
            
            user_id = session['user_id']
            username = session['username']
            
            print("\n" + "="*70)
            print("NUEVA SOLICITUD DE LOTE")
            print("="*70)
            print(f"Usuario: {username} (ID: {user_id})")
            print(f"Lote: {batch_name}")
            
            # DECODIFICAR IMÁGENES (✅ OPTIMIZADO: SIN GZIP)
            try:
                images_json_decoded = base64.b64decode(images_json).decode('utf-8')
                images = json.loads(images_json_decoded)
            except Exception as e:
                print(f"[SERVIDOR] Error decodificando: {e}")
                raise Exception(f"Error al decodificar imágenes: {str(e)}")
            
            total_images = len(images)
            print(f"Total de imágenes: {total_images}")
            
            # CREAR LOTE EN DB###########################################################################################################################
            success, batch_data = rest_client.create_batch(user_id, batch_name)
            if not success:
                raise Exception(f"Error al crear lote: {batch_data.get('error')}")
            
            batch_id = batch_data['batch_id']
            print(f"Lote creado en DB: batch_id={batch_id}")
            
            # Log inicial##################################################################################################################
            rest_client.create_log(
                batch_id=batch_id,
                log_level='info',
                message=f'Lote "{batch_name}" iniciado con {total_images} imágenes por usuario {username}'
            )
            #########################################################################################################################################
            rest_client.update_batch_status(batch_id, 'processing')
            
            # ✅ REGISTRAR IMÁGENES Y PREPARAR TRABAJOS (OPTIMIZADO CON BATCH INSERT)
            print("\nPreparando trabajos...")
            jobs = []
            batch_images = []  # Para batch insert
            
            for idx, image_data in enumerate(images, 1):
                filename = image_data['filename']
                image_base64 = image_data['image_data_base64']
                transformations = image_data.get('transformations', [])
                
                if len(transformations) > 5:
                    print(f"  ⚠ Imagen {filename}: Limitando a 5 transformaciones")
                    transformations = transformations[:5]
                
                try:
                    # Decodificar imagen
                    image_bytes = base64.b64decode(image_base64)
                    file_size = len(image_bytes)
                    
                    # Guardar temporalmente con nombre único
                    temp_dir = tempfile.gettempdir()
                    # Agregar batch_id + timestamp para evitar colisiones
                    unique_filename = f"batch_{batch_id}_{idx}_{filename}"
                    image_path = os.path.join(temp_dir, unique_filename)
                    with open(image_path, "wb") as f:
                        f.write(image_bytes)
                    
                    # Preparar para batch insert
                    batch_images.append({
                        'original_filename': filename,
                        'storage_path': image_path,
                        'file_size': file_size,
                        'transformations': [
                            {
                                'name': t.get('name', ''),
                                'parameters': t.get('parameters', {}),
                                'execution_order': order
                            }
                            for order, t in enumerate(transformations, 1)
                        ]
                    })
                    
                    # Guardar info para jobs (sin image_id aún)
                    jobs.append({
                        'image_path': image_path,
                        'filename': filename,
                        'file_size': file_size,
                        'transformations': transformations,
                        'batch_id': batch_id,
                        'idx': idx  # Índice para matching después
                    })
                    
                    print(f"  ✓ Trabajo {idx}/{total_images}: {filename} "
                          f"({load_balancer._format_bytes(file_size)})")
                    
                except Exception as e:
                    print(f"  ✗ Error preparando imagen {idx}: {e}")
                    rest_client.create_log(
                        batch_id=batch_id,
                        log_level='error',
                        message=f'Error preparando {filename}: {str(e)}'
                    )
            
            # BATCH INSERT: 1 sola llamada REST en lugar de 30+
            if batch_images:
######################################################################################################################################################
                success, batch_result = rest_client.create_images_batch(
                    batch_id=batch_id,
                    images=batch_images
                )
                
                if success:
                    image_ids = batch_result.get('image_ids', [])
                    # Asignar image_ids a los jobs
                    for i, job in enumerate(jobs):
                        if i < len(image_ids):
                            job['image_id'] = image_ids[i]
                    print(f"✅ Batch insert: {len(batch_images)} imágenes + {batch_result.get('transformations_created', 0)} transformaciones")
                else:
                    print(f"❌ Error en batch insert: {batch_result.get('error')}")
            
            print(f"\n{len(jobs)} trabajos listos para distribución")
            
            # DISTRIBUIR TRABAJOS POR PESO
            print("\n" + "="*70)
            print("DISTRIBUYENDO TRABAJOS POR PESO")
            print("="*70)
            
            try:
 ####################################################################################################################################################################               
                job_assignments = load_balancer.distribute_jobs(jobs)
                
                # ⭐ MOSTRAR RESUMEN DE DISTRIBUCIÓN
                print("\n📊 RESUMEN DE DISTRIBUCIÓN:")
                print("-" * 70)
                
                # Agrupar por nodo
                distribution = {}
                for job, node in job_assignments:
                    node_id = node['node_id']
                    if node_id not in distribution:
                        distribution[node_id] = {
                            'node_name': node['node_name'],
                            'node_address': f"{node['ip_address']}:{node['port']}",
                            'images': [],
                            'total_weight': 0
                        }
                    
                    distribution[node_id]['images'].append({
                        'filename': job['filename'],
                        'size': job['file_size']
                    })
                    distribution[node_id]['total_weight'] += job['file_size']
                
                # Mostrar distribución por nodo
                for node_id in sorted(distribution.keys()):
                    node_info = distribution[node_id]
                    print(f"\n🖥️  {node_info['node_name']} ({node_info['node_address']})")
                    print(f"   Peso total: {load_balancer._format_bytes(node_info['total_weight'])}")
                    print(f"   Imágenes asignadas: {len(node_info['images'])}")
                    print(f"   Lista:")
                    
                    for idx, img in enumerate(node_info['images'], 1):
                        print(f"      {idx}. {img['filename']} ({load_balancer._format_bytes(img['size'])})")
                
                print("\n" + "-" * 70)
                print(f"✅ Distribución completada: {len(jobs)} imágenes en {len(distribution)} nodos")
                print("=" * 70)
                
            except Exception as e:
                print(f"[SERVIDOR] Error en distribución: {e}")
                raise
            
            # PROCESAR EN PARALELO
            print("\n" + "="*70)
            print("INICIANDO PROCESAMIENTO PARALELO")
            print("="*70)
            
            futures = []
            for job, node in job_assignments:
                job['assigned_node'] = node
####################################################################################################################################                
                future = thread_pool.submit(self._delegate_to_node, job)
                futures.append(future)
 ##########################################################################################################################           
            # ESPERAR RESULTADOS
            processed_count = 0
            failed_count = 0
            
            for idx, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                    if result['success']:
                        processed_count += 1
                        print(f"[{idx}/{len(jobs)}] ✓ {result['filename']} completado")
                    else:
                        failed_count += 1
                        print(f"[{idx}/{len(jobs)}] ✗ {result['filename']} falló")
                except Exception as e:
                    print(f"[{idx}/{len(jobs)}] ✗ Error en thread: {e}")
                    failed_count += 1
            
            # FINALIZAR
            final_status = 'completed' if failed_count == 0 else ('failed' if processed_count == 0 else 'completed')
            rest_client.update_batch_status(batch_id, final_status, processed_images=processed_count)
            
            processing_time = int((time.time() - start_time) * 1000)
            
            # Log final
            rest_client.create_log(
                batch_id=batch_id,
                log_level='info',
                message=f'Lote completado: {processed_count}/{total_images} exitosas, '
                        f'{failed_count} fallidas en {processing_time}ms'
            )
            
            print("\n" + "="*70)
            print("RESUMEN DEL LOTE")
            print("="*70)
            print(f"Total:        {total_images}")
            print(f"Exitosas:     {processed_count}")
            print(f"Fallidas:     {failed_count}")
            print(f"Tiempo total: {processing_time} ms")
            print("="*70 + "\n")
            
            download_url = f'http://localhost:5000/api/batches/{batch_id}/download'
            print(f"Descarga disponible en: {download_url}\n")
            
            return {
                'success': True,
                'message': f'Lote procesado: {processed_count}/{total_images} exitosas',
                'batch_id': batch_id,
                'total_images': total_images,
                'processed_images': processed_count,
                'failed_images': failed_count,
                'processing_time_ms': processing_time,
                'download_url': download_url
            }
            
        except Exception as e:
            print(f"\n[SERVIDOR] ERROR CRÍTICO: {str(e)}")
            import traceback
            traceback.print_exc()
            
            processing_time = int((time.time() - start_time) * 1000)
            
            return {
                'success': False,
                'message': f'Error en el servidor: {str(e)}',
                'batch_id': 0,
                'total_images': 0,
                'processed_images': 0,
                'failed_images': 0,
                'processing_time_ms': processing_time,
                'download_url': ''
            }
    
    def _delegate_to_node(self, job):
        """Delega procesamiento a un nodo vía gRPC"""
        image_id = job['image_id']
        image_path = job['image_path']
        filename = job['filename']
        transformations = job['transformations']
        batch_id = job['batch_id']
        node = job['assigned_node']
        
        thread_name = threading.current_thread().name
        file_size = job.get('file_size', 0)
        
        print(f"\n[{thread_name}] Delegando {filename} "
              f"({load_balancer._format_bytes(file_size)})")
        
        try:
            node_id = node['node_id']
            node_address = f"{node['ip_address']}:{node['port']}"
            
            print(f"[{thread_name}] Nodo asignado: {node['node_name']} ({node_address})")
            
            rest_client.create_log(
                batch_id=batch_id,
                image_id=image_id,
                node_id=node_id,
                log_level='info',
                message=f'Delegando {filename} ({load_balancer._format_bytes(file_size)}) a {node["node_name"]}'
            )
            
            # DELEGAR AL NODO VÍA gRPC
##########################################################################################################################################            
            client = NodeClient(node_address)
            result = client.process_image(image_id, image_path, transformations)
            client.close()
###################################################################################################################################################            
            if result['success']:
                # CREAR DIRECTORIO POR BATCH
                batch_output_dir = os.path.join(
                    os.path.dirname(__file__), 
                    'output', 
                    f'batch_{batch_id}'
                )
                os.makedirs(batch_output_dir, exist_ok=True)
                
                # NOMBRE DEL ARCHIVO
                name_without_ext = os.path.splitext(filename)[0]
                extension = os.path.splitext(filename)[1]
                result_filename = f"{name_without_ext}_processed{extension}"
                result_path = os.path.join(batch_output_dir, result_filename)
                
                # GUARDAR IMAGEN RECIBIDA
                with open(result_path, 'wb') as f:
                    f.write(result['image_data'])
                
                print(f"[{thread_name}] Imagen guardada en: {result_path}")
                
                # REGISTRAR EN DB
                relative_path = f'batch_{batch_id}/{result_filename}'
                
                rest_client.add_image_result(
                    image_id=image_id,
                    node_id=node_id,
                    result_filename=result_filename,
                    storage_path=relative_path,
                    processing_time_ms=result['processing_time_ms'],
                    status='success'
                )
                
                rest_client.mark_image_processed(image_id)
                
                rest_client.create_log(
                    batch_id=batch_id,
                    image_id=image_id,
                    node_id=node_id,
                    log_level='info',
                    message=f'{filename} procesado exitosamente en {result["processing_time_ms"]}ms'
                )
                
                print(f"[{thread_name}] ✓ {filename} completado ({result['processing_time_ms']}ms)")
            else:
                rest_client.add_image_result(
                    image_id=image_id,
                    node_id=node_id,
                    result_filename='',
                    storage_path='',
                    processing_time_ms=result['processing_time_ms'],
                    status='failed',
                    error_message=result['error_message']
                )
                
                rest_client.create_log(
                    batch_id=batch_id,
                    image_id=image_id,
                    node_id=node_id,
                    log_level='error',
                    message=f'{filename} falló: {result["error_message"]}'
                )
                
                print(f"[{thread_name}] ✗ {filename} falló: {result['error_message']}")
            
            return {'success': result['success'], 'filename': filename}
            
        except Exception as e:
            print(f"[{thread_name}] ✗ Error delegando {filename}: {e}")
            
            rest_client.create_log(
                batch_id=batch_id,
                image_id=image_id,
                log_level='error',
                message=f'Error delegando {filename}: {str(e)}'
            )
            
            return {'success': False, 'filename': filename}
    
    def create_soap_response(self, response_type, result):
        """Crear respuesta SOAP según el tipo"""
        if response_type == 'Register':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <RegisterResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <message>{result['message']}</message>
      <user_id>{result['user_id']}</user_id>
    </RegisterResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'Login':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <LoginResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <message>{result['message']}</message>
      <session_token>{result['session_token']}</session_token>
      <user_id>{result['user_id']}</user_id>
    </LoginResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'Logout':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <LogoutResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <message>{result['message']}</message>
    </LogoutResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'ProcessBatch':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ProcessBatchResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <message>{result['message']}</message>
      <batch_id>{result['batch_id']}</batch_id>
      <total_images>{result['total_images']}</total_images>
      <processed_images>{result['processed_images']}</processed_images>
      <failed_images>{result['failed_images']}</failed_images>
      <processing_time_ms>{result['processing_time_ms']}</processing_time_ms>
      <download_url>{result.get('download_url', '')}</download_url>
    </ProcessBatchResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'GetNodesMetrics':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetNodesMetricsResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <nodes_json>{result['nodes_json']}</nodes_json>
    </GetNodesMetricsResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'GetBatchMetrics':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetBatchMetricsResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <metrics_json>{result['metrics_json']}</metrics_json>
    </GetBatchMetricsResponse>
  </soap:Body>
</soap:Envelope>'''
        
        elif response_type == 'NodeHeartbeat':
            return f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <NodeHeartbeatResponse xmlns="http://example.org/ImageProcessingService.wsdl">
      <success>{str(result['success']).lower()}</success>
      <message>{result['message']}</message>
    </NodeHeartbeatResponse>
  </soap:Body>
</soap:Envelope>'''

def run_server(host='0.0.0.0', port=8000):
    """FUNCIÓN DE INICIALIZACIÓN DEL SERVIDOR SOAP"""
    server_address = (host, port)
    httpd = HTTPServer(server_address, SOAPHandler)
    
    print("\n" + "="*70)
    print("SERVIDOR SOAP CON PROCESAMIENTO PARALELO")
    print("="*70)
    print(f"Dirección:        http://{host}:{port}/")
    print(f"WSDL:             http://{host}:{port}/wsdl")
    print(f"Endpoint SOAP:    http://{host}:{port}/soap")
    print(f"")
    print(f"Características:")
    print(f"  ✓ Procesamiento paralelo (ThreadPool: {thread_pool._max_workers} workers)")
    print(f"  ✓ Load Balancing por peso de imágenes")
    print(f"  ✓ Sistema de logs completo en DB")
    print(f"  ✓ Heartbeat de nodos cada 30s")
    print(f"  ✓ Distribución equitativa entre nodos")
    print(f"  ✓ Registro de transformaciones en DB")
    print(f"  ✓ Descarga de resultados en ZIP por lote")
    print(f"  ✓ Imágenes organizadas por batch")
    print(f"  ✓ Métricas de consumo (GetNodesMetrics, GetBatchMetrics)")
    print("="*70)
    print("Presione Ctrl+C para detener el servidor")
    print("="*70 + "\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Deteniendo servidor...")
        thread_pool.shutdown(wait=True)
        httpd.server_close()
        print("[SERVIDOR] Servidor detenido correctamente.")

if __name__ == "__main__":
    run_server()