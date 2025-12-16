# Archivo: server/rest_client.py
# ACTUALIZADO: Agregar métodos para usuarios

import requests
import json
from typing import Dict, List, Optional

class RestClient:
    """Cliente REST para interactuar con el servicio DB"""
    
    def __init__(self, base_url='http://localhost:5000'):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def _make_request(self, method, endpoint, data=None, params=None):
        """Método interno para hacer requests HTTP"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == 'GET':
                response = self.session.get(url, params=params)
            elif method == 'POST':
                response = self.session.post(url, json=data)
            elif method == 'PUT':
                response = self.session.put(url, json=data)
            elif method == 'DELETE':
                response = self.session.delete(url)
            
            response.raise_for_status()
            return True, response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"[REST CLIENT] Error: {e}")
            return False, {'error': str(e)}
    
    # ============= MÉTODOS PARA USUARIOS (NUEVO) =============
    
    def register_user(self, username: str, password: str, email: str,
                     first_name: str = None, last_name: str = None):
        """Registrar nuevo usuario"""
        data = {
            'username': username,
            'password': password,
            'email': email,
            'first_name': first_name,
            'last_name': last_name
        }
        return self._make_request('POST', '/api/users/register', data=data)
    
    def login_user(self, username: str, password: str):
        """Verificar credenciales de usuario"""
        data = {
            'username': username,
            'password': password
        }
        return self._make_request('POST', '/api/users/login', data=data)
    
    def get_user(self, user_id: int):
        """Obtener información de usuario"""
        return self._make_request('GET', f'/api/users/{user_id}')
    
    # ============= MÉTODOS PARA BATCHES =============
    
    def create_batch(self, user_id: int, batch_name: str, 
                    output_format='jpg', compression_type='zip'):
        """Crear nuevo lote"""
        data = {
            'user_id': user_id,
            'batch_name': batch_name,
            'output_format': output_format,
            'compression_type': compression_type
        }
        return self._make_request('POST', '/api/batches', data=data)
    
    def get_batch(self, batch_id: int):
        """Obtener información de lote"""
        return self._make_request('GET', f'/api/batches/{batch_id}')
    
    def update_batch_status(self, batch_id: int, status: str, 
                           processed_images: Optional[int] = None):
        """Actualizar estado de lote"""
        data = {'status': status}
        if processed_images is not None:
            data['processed_images'] = processed_images
        return self._make_request('PUT', f'/api/batches/{batch_id}/status', data=data)
    
    # ============= MÉTODOS PARA IMAGES =============
    
    def create_image(self, batch_id: int, original_filename: str, 
                    storage_path: str, file_size: int = None,
                    width: int = None, height: int = None, 
                    format: str = None):
        """Registrar nueva imagen"""
        data = {
            'batch_id': batch_id,
            'original_filename': original_filename,
            'storage_path': storage_path,
            'file_size': file_size,
            'width': width,
            'height': height,
            'format': format
        }
        return self._make_request('POST', '/api/images', data=data)
    
    def get_image(self, image_id: int):
        """Obtener información de imagen"""
        return self._make_request('GET', f'/api/images/{image_id}')
    
    def create_images_batch(self, batch_id: int, images: list):
        """
        Registrar múltiples imágenes con sus transformaciones (OPTIMIZADO)
        
        Args:
            batch_id: ID del lote
            images: Lista de diccionarios con:
                - original_filename: str
                - storage_path: str
                - file_size: int
                - transformations: lista de {name, parameters, execution_order}
        
        Returns:
            (success, result_dict)
        """
        data = {
            'batch_id': batch_id,
            'images': images
        }
        return self._make_request('POST', '/api/images/batch', data=data)
    
    def add_image_result(self, image_id: int, node_id: int, 
                        result_filename: str, storage_path: str,
                        processing_time_ms: int, status: str = 'success',
                        error_message: str = '', **kwargs):
        """Registrar resultado procesado"""
        data = {
            'node_id': node_id,
            'result_filename': result_filename,
            'storage_path': storage_path,
            'processing_time_ms': processing_time_ms,
            'status': status,
            'error_message': error_message,
            **kwargs
        }
        return self._make_request('POST', f'/api/images/{image_id}/result', data=data)
    
    # ============= MÉTODOS PARA NODES =============
    
    def get_active_nodes(self):
        """Obtener nodos activos"""
        return self._make_request('GET', '/api/nodes/active')
    
    def update_node_heartbeat(self, node_id: int, ip_address: str = None, port: int = None, 
                            cpu_cores: int = None, ram_gb: float = None, 
                            current_load: int = None, status: str = None):
        """Actualizar heartbeat de nodo"""
        data = {}
        if status:
            data['status'] = status
        if ip_address:
            data['ip_address'] = ip_address
        if port:
            data['port'] = port
        if cpu_cores:
            data['cpu_cores'] = cpu_cores
        if ram_gb:
            data['ram_gb'] = ram_gb
        if current_load is not None:
            data['current_load'] = current_load
        return self._make_request('PUT', f'/api/nodes/{node_id}/heartbeat', data=data)
    
    # ============= MÉTODOS PARA TRANSFORMATIONS =============
    
    def get_all_transformations(self):
        """Obtener todas las transformaciones"""
        return self._make_request('GET', '/api/transformations')
    
    def get_transformation_by_name(self, name: str):
        """Obtener transformación por nombre"""
        return self._make_request('GET', f'/api/transformations/by-name/{name}')

    # ============= MÉTODOS PARA LOGS =============
    
    def create_log(self, node_id=None, batch_id=None, image_id=None, 
                   log_level='info', message=''):
        """Crear log en execution_logs"""
        data = {
            'node_id': node_id,
            'batch_id': batch_id,
            'image_id': image_id,
            'log_level': log_level,  # 'info', 'warning', 'error', 'debug'
            'message': message
        }
        return self._make_request('POST', '/api/logs', data=data)

    def get_batch_logs(self, batch_id):
        """Obtener logs de un lote"""
        return self._make_request('GET', f'/api/logs/batch/{batch_id}')

    # ============= MÉTODOS ADICIONALES =============
    
    def add_image_transformation(self, image_id: int, transformation_name: str, 
                                 parameters: dict, execution_order: int):
        """Agregar transformación a una imagen"""
        data = {
            'image_id': image_id,
            'transformation_name': transformation_name,
            'parameters': parameters,
            'execution_order': execution_order
        }
        return self._make_request('POST', '/api/image-transformations', data=data)

    def mark_image_processed(self, image_id: int):
        """Marcar imagen como procesada (actualiza processed_at)"""
        return self._make_request('PUT', f'/api/images/{image_id}/processed')