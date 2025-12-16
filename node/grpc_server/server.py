# Archivo: node/grpc_server/server.py
# SERVIDOR gRPC DEL NODO DE PROCESAMIENTO

import grpc
from concurrent import futures
import json
import time
import os
import sys
import psutil
import tempfile

# ⭐ AGREGAR RUTA A PROTOS
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'protos'))

# ⭐ IMPORTAR PROTOBUF
import image_processing_pb2
import image_processing_pb2_grpc

from transformations.image_ops import ImageProcessor

class ImageProcessorServicer(image_processing_pb2_grpc.ImageProcessorServicer):
    """CLASE PRINCIPAL DEL SERVICIO gRPC"""
    
    def ProcessImage(self, request, context):
        """Procesa una imagen según las transformaciones solicitadas"""
        print("\n=== [NODO] NUEVA SOLICITUD DE PROCESAMIENTO RECIBIDA ===")
        print(f"[NODO] ID de imagen: {request.image_id}")
        print(f"[NODO] Filename: {request.filename}")
        print(f"[NODO] Image data size: {len(request.image_data)} bytes")
        print(f"[NODO] Número de transformaciones: {len(request.transformations)}")
        
        # CONVERSIÓN DE DATOS
        transformations = []
        for i, t in enumerate(request.transformations):
            print(f"[NODO] → Transformación {i+1}: {t.name} (ID: {t.transformation_id})")
            transformations.append({
                'transformation_id': t.transformation_id,
                'name': t.name,
                'parameters': t.parameters
            })
        
        # GUARDAR IMAGEN TEMPORALMENTE DESDE BYTES
        temp_dir = tempfile.gettempdir()
        temp_image_path = os.path.join(temp_dir, f"grpc_{request.image_id}_{request.filename}")
        
        try:
            with open(temp_image_path, 'wb') as f:
                f.write(request.image_data)
            print(f"[NODO] Imagen temporal guardada en: {temp_image_path}")
        except Exception as e:
            print(f"[NODO] Error guardando imagen temporal: {e}")
            return image_processing_pb2.ProcessResponse(
                success=False,
                result_path='',
                error_message=f'Error guardando imagen temporal: {str(e)}',
                processing_time_ms=0,
                image_data=b''
            )
        
        # PROCESAMIENTO
        print(f"[NODO] Iniciando procesamiento de imagen...")
        start_time = time.time()
        
   ##############################################################################################################################################
        result = ImageProcessor.process_image(
   ####################################################################################################################################################         
            image_path=temp_image_path,
            transformations=transformations
        )
        
        processing_time = time.time() - start_time
        print(f"[NODO] Procesamiento completado en {processing_time*1000:.2f} ms")
        print(f"[NODO] Resultado: {'Éxito' if result['success'] else 'Error'}")
        
        # LEER IMAGEN PROCESADA
        image_bytes = b''
        
        if result['success']:
            print(f"[NODO] Imagen resultado guardada en: {result['result_path']}")
            
            try:
                with open(result['result_path'], 'rb') as f:
                    image_bytes = f.read()
                
                print(f"[NODO] ✓ Imagen leída: {len(image_bytes)} bytes ({len(image_bytes)/1024:.2f} KB)")
                
            except Exception as e:
                print(f"[NODO] ✗ Error al leer imagen: {e}")
                result['success'] = False
                result['error_message'] = f"Error al leer imagen procesada: {str(e)}"
        else:
            print(f"[NODO] Error: {result['error_message']}")
        
        # LIMPIAR ARCHIVO TEMPORAL
        try:
            if os.path.exists(temp_image_path):
                os.remove(temp_image_path)
                print(f"[NODO] Archivo temporal eliminado: {temp_image_path}")
        except Exception as e:
            print(f"[NODO] Advertencia: No se pudo eliminar temp file: {e}")
        
        # RESPUESTA
        response = image_processing_pb2.ProcessResponse(
            success=result['success'],
            result_path=result['result_path'],
            error_message=result['error_message'],
            processing_time_ms=result['processing_time_ms'],
            image_data=image_bytes
        )
        
        print(f"[NODO] Enviando respuesta al servidor con imagen incluida")
        return response
    
    def GetNodeStatus(self, request, context):
        """Retorna el estado actual del nodo"""
        print(f"[NODO] Solicitud de estado recibida para nodo ID: {request.node_id}")
        
        cpu_usage = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        memory_usage = memory.percent
        
        status = 'active'
        if cpu_usage > 90 or memory_usage > 90:
            status = 'error'
        
        print(f"[NODO] Estado: {status}, CPU: {cpu_usage}%, Memoria: {memory_usage}%")
        
        response = image_processing_pb2.StatusResponse(
            status=status,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage
        )
        
        return response

def serve(port=50051):
    """Inicializa el servidor gRPC"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    image_processing_pb2_grpc.add_ImageProcessorServicer_to_server(
        ImageProcessorServicer(),
        server
    )
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"Servidor gRPC iniciado en puerto {port}")
    return server