# Archivo: server/grpc_client/client.py
# CLIENTE gRPC PARA COMUNICACIÓN CON NODOS

import grpc
import json
import time
import sys
import os

# Agregar ruta a protos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'protos'))

import image_processing_pb2
import image_processing_pb2_grpc

class NodeClient:
    """CLIENTE gRPC PARA COMUNICACIÓN CON NODOS"""
    
    def __init__(self, node_address):
        """Establece conexión con un nodo específico"""
        self.channel = grpc.insecure_channel(node_address)
        self.stub = image_processing_pb2_grpc.ImageProcessorStub(self.channel)
    
    def process_image(self, image_id, image_path, transformations):
        """Envía una solicitud para procesar una imagen al nodo
        
        Args:
            image_id: ID de la imagen
            image_path: Ruta al archivo de imagen (se leerá y enviará como bytes)
            transformations: Lista de transformaciones a aplicar
        """
        filename = os.path.basename(image_path)
        print(f"[CLIENTE gRPC] Preparando solicitud gRPC para imagen: {filename}")
        
        # LEER IMAGEN COMO BYTES
        try:
            with open(image_path, 'rb') as f:
                image_bytes = f.read()
            print(f"[CLIENTE gRPC] Imagen cargada: {len(image_bytes)} bytes")
        except Exception as e:
            print(f"[CLIENTE gRPC] Error leyendo imagen: {e}")
            return {
                'success': False,
                'result_path': '',
                'error_message': f'Error leyendo imagen: {str(e)}',
                'processing_time_ms': 0,
                'image_data': b''
            }
        
        # CONVERSIÓN DE TRANSFORMACIONES
        proto_transformations = []
        print(f"[CLIENTE gRPC] Convirtiendo {len(transformations)} transformaciones")
        
        for t in transformations:
            try:
                transformation_id = int(t.get('transformation_id', 0)) if isinstance(t, dict) else 0
                name = t.get('name', '') if isinstance(t, dict) else str(t)
                
                parameters = t.get('parameters', '{}') if isinstance(t, dict) else '{}'
                if isinstance(parameters, dict):
                    parameters = json.dumps(parameters)
                
                print(f"[CLIENTE gRPC] → Transformación: {name} (ID: {transformation_id})")
                
                proto_t = image_processing_pb2.Transformation(
                    transformation_id=transformation_id,
                    name=name,
                    parameters=parameters
                )
                proto_transformations.append(proto_t)
            except Exception as e:
                print(f"[CLIENTE gRPC] Error al procesar transformación: {e}")
                continue
        
        # CREAR SOLICITUD CON BYTES
        request = image_processing_pb2.ProcessRequest(
            image_id=image_id,
            image_path=image_path,  # mantener por compatibilidad
            filename=filename,       # nuevo: nombre del archivo
            image_data=image_bytes,  # nuevo: bytes de la imagen
            transformations=proto_transformations
        )
        
        try:
            print(f"[CLIENTE gRPC] Enviando solicitud al nodo...")
            
            # ENVIAR SOLICITUD
####################################################################################################################################
            response = self.stub.ProcessImage(request)
            
            print(f"[CLIENTE gRPC] Respuesta recibida del nodo")
            print(f"[CLIENTE gRPC] → Éxito: {response.success}")
            
            if response.success and response.image_data:
                print(f"[CLIENTE gRPC] → Imagen recibida: {len(response.image_data)} bytes")
            
            # RETORNAR RESULTADO
            return {
                'success': response.success,
                'result_path': response.result_path,
                'error_message': response.error_message,
                'processing_time_ms': response.processing_time_ms,
                'image_data': response.image_data
            }
            
        except grpc.RpcError as e:
            print(f"[CLIENTE gRPC] Error RPC: {e.details()}")
            return {
                'success': False,
                'error_message': f"Error de comunicación: {e.details()}",
                'result_path': '',
                'processing_time_ms': 0,
                'image_data': b''
            }
        except Exception as e:
            print(f"[CLIENTE gRPC] Error: {e}")
            return {
                'success': False,
                'error_message': f"Error: {str(e)}",
                'result_path': '',
                'processing_time_ms': 0,
                'image_data': b''
            }
    
    def get_node_status(self, node_id):
        """Obtiene el estado actual del nodo"""
        request = image_processing_pb2.StatusRequest(node_id=node_id)
        
        try:
######################################################################################################################################            
            response = self.stub.GetNodeStatus(request)
######################################################################################################################################
            return {
                'status': response.status,
                'cpu_usage': response.cpu_usage,
                'memory_usage': response.memory_usage
            }
        except grpc.RpcError as e:
            print(f"[CLIENTE gRPC] Error RPC: {e.details()}")
            return {
                'status': 'error',
                'cpu_usage': 0,
                'memory_usage': 0
            }
        except Exception as e:
            print(f"[CLIENTE gRPC] Error: {e}")
            return {
                'status': 'error',
                'cpu_usage': 0,
                'memory_usage': 0
            }
    
    def close(self):
        """Cierra la conexión gRPC"""
        if hasattr(self, 'channel'):
            self.channel.close()