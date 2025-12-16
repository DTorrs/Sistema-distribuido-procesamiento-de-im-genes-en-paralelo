# Archivo: server/load_balancer.py
# BALANCEADOR DE CARGA CON DISTRIBUCIÓN POR PESO Y HEALTH CHECK

import threading
from datetime import datetime, timedelta

class LoadBalancer:
    """
    Distribuye trabajo entre nodos balanceando el PESO TOTAL
    Usa algoritmo de Greedy Partition para balance equitativo
    ⭐ CON VERIFICACIÓN DE CONECTIVIDAD EN TIEMPO REAL
    """
    
    def __init__(self, rest_client):
        self.rest_client = rest_client
        self.nodes_cache = []
        self.cache_lock = threading.Lock()
        self.cache_time = None
        self.cache_ttl = 30  # segundos
    
    def _check_node_health(self, node):
        """
        ⭐ NUEVO: Verifica si un nodo está activo mediante gRPC GetNodeStatus
        
        Args:
            node: Diccionario con info del nodo
            
        Returns:
            bool: True si el nodo responde, False si no
        """
        try:
            # Importar cliente gRPC
            from grpc_client.client import NodeClient
            
            node_address = f"{node['ip_address']}:{node['port']}"
            
            # Crear cliente gRPC con timeout corto
            client = NodeClient(node_address)
            
            # Llamar GetNodeStatus (si falla, lanza excepción)
 ####################################################################################################################################################           
            status = client.get_node_status(node['node_id'])
  ###################################################################################################################################################          
            # Cerrar conexión
            client.close()
            
            # Si llegamos aquí, el nodo responde
            return True
            
        except Exception as e:
            # Cualquier error = nodo no disponible
            # print(f"[LOAD BALANCER]   Health check failed: {e}")
            return False
    
    def get_available_nodes(self):
        """
        Obtiene lista de nodos activos (con caché)
        ⭐ ACTUALIZADO: Verifica conectividad real antes de agregar al caché
        """
        
        with self.cache_lock:
            # Verificar si el caché es válido
            if (self.cache_time and 
                datetime.now() - self.cache_time < timedelta(seconds=self.cache_ttl) and
                self.nodes_cache):
                return self.nodes_cache
            
            # Refrescar caché
            print("[LOAD BALANCER] Actualizando lista de nodos...")
            
 ###########################################################################################################################
            success, all_nodes = self.rest_client.get_active_nodes()

            
            if not success or not all_nodes:
                print(f"[LOAD BALANCER] ⚠ No se pudieron obtener nodos de la DB")
                return []
            
            # ⭐ NUEVO: Verificar conectividad de cada nodo
            verified_nodes = []
            
            print(f"[LOAD BALANCER] Verificando {len(all_nodes)} nodos...")
            
            for node in all_nodes:
                node_address = f"{node['ip_address']}:{node['port']}"

 ########################################################################################################################               
                # Health check directo vía gRPC
                if self._check_node_health(node):
                    verified_nodes.append(node)
####################################################################################################################################                    
                    print(f"[LOAD BALANCER]   ✓ {node['node_name']} ({node_address}): ACTIVO")
                else:
                    print(f"[LOAD BALANCER]   ✗ {node['node_name']} ({node_address}): NO RESPONDE")
            
            # Eliminar duplicados por puerto (por si acaso)
            seen_ports = {}
            unique_nodes = []
            
            for node in verified_nodes:
                port_key = f"{node['ip_address']}:{node['port']}"
                
                if port_key not in seen_ports:
                    seen_ports[port_key] = node
                    unique_nodes.append(node)
                else:
                    # Si hay duplicado, mantener el más reciente
                    existing = seen_ports[port_key]
                    existing_hb = existing.get('last_heartbeat', '')
                    current_hb = node.get('last_heartbeat', '')
                    
                    if current_hb > existing_hb:
                        unique_nodes.remove(existing)
                        unique_nodes.append(node)
                        seen_ports[port_key] = node
                        print(f"[LOAD BALANCER]   ⚠ Puerto duplicado {port_key}: "
                              f"Usando {node['node_name']} (más reciente)")
            
            # Actualizar caché
            self.nodes_cache = unique_nodes
            self.cache_time = datetime.now()
            
            if len(unique_nodes) == 0:
                print(f"[LOAD BALANCER] ⚠ NO HAY NODOS ACTIVOS")
            else:
                print(f"[LOAD BALANCER] ✅ {len(unique_nodes)} nodos activos verificados:")
                for node in unique_nodes:
                    print(f"  - {node['node_name']}: {node['ip_address']}:{node['port']} "
                          f"(weight: {node.get('weight', 1)})")
            
            return unique_nodes
    
    def distribute_jobs(self, jobs):
        """
        Distribuye trabajos entre nodos balanceando el PESO TOTAL
        
        Args:
            jobs: Lista de trabajos con estructura:
                [
                    {
                        'image_id': int,
                        'filename': str,
                        'file_size': int,  ← IMPORTANTE
                        'transformations': list,
                        ...
                    }
                ]
        
        Returns:
            Lista de tuplas (job, node):
                [
                    (job1, node1),
                    (job2, node1),
                    (job3, node2),
                    ...
                ]
        """
        #########################################################################################################################
        nodes = self.get_available_nodes()
        ##########################################################################################################################
        if not nodes:
            raise Exception("No hay nodos disponibles")
        
        print(f"\n[LOAD BALANCER] Distribuyendo {len(jobs)} trabajos entre {len(nodes)} nodos")
        print("[LOAD BALANCER] Algoritmo: Greedy Partition by Weight")
        
        # Inicializar bins (uno por nodo)
        bins = []
        for node in nodes:
            weight_factor = node.get('weight', 1)
            bins.append({
                'node': node,
                'jobs': [],
                'total_weight': 0,
                'weight_factor': weight_factor
            })
        
        # Ordenar trabajos por tamaño (descendente) para mejor distribución
        sorted_jobs = sorted(jobs, key=lambda x: x.get('file_size', 0), reverse=True)
        
        total_size = sum(job.get('file_size', 0) for job in sorted_jobs)
        print(f"[LOAD BALANCER] Peso total: {self._format_bytes(total_size)}")
        
        # Algoritmo Greedy: Asignar cada trabajo al bin con menos peso acumulado
        for job in sorted_jobs:
            # Encontrar el bin con menor peso (considerando el weight_factor del nodo)
            min_bin = min(bins, key=lambda b: b['total_weight'] / b['weight_factor'])
            
            # Asignar trabajo a ese bin
            min_bin['jobs'].append(job)
            min_bin['total_weight'] += job.get('file_size', 0)
        
        # Mostrar distribución
        print("\n[LOAD BALANCER] Distribución resultante:")
        for i, bin_info in enumerate(bins):
            node = bin_info['node']
            num_jobs = len(bin_info['jobs'])
            total_weight = bin_info['total_weight']
            percentage = (total_weight / total_size * 100) if total_size > 0 else 0
            
            print(f"  Nodo {i+1} ({node['node_name']}):")
            print(f"    • Trabajos:    {num_jobs}")
            print(f"    • Peso total:  {self._format_bytes(total_weight)} ({percentage:.1f}%)")
            print(f"    • Weight:      {node.get('weight', 1)}")
        
        # Convertir a lista de tuplas (job, node)
        assignments = []
        for bin_info in bins:
            for job in bin_info['jobs']:
                assignments.append((job, bin_info['node']))
        
        print(f"\n[LOAD BALANCER] ✓ {len(assignments)} trabajos distribuidos")
        
        return assignments
    
    def select_node(self):
        """
        Método legacy para compatibilidad
        Selecciona un nodo usando Weighted Least Connections
        (Se usa solo cuando no se puede hacer distribución por peso)
        """
        nodes = self.get_available_nodes()
        
        if not nodes:
            print("[LOAD BALANCER] ✗ No hay nodos disponibles")
            return None
        
        # Algoritmo de Weighted Least Connections
        best_node = None
        best_score = float('inf')
        
        for node in nodes:
            weight = node.get('weight', 1)
            max_jobs = node.get('max_concurrent_jobs', 5)
            current_load = node.get('current_load', 0)
            
            if weight == 0 or max_jobs == 0:
                continue
            
            score = current_load / (weight * max_jobs)
            
            if score < best_score:
                best_score = score
                best_node = node
        
        if best_node:
            print(f"[LOAD BALANCER] ✓ Nodo seleccionado: {best_node['node_name']}")
        
        return best_node
    
    def get_node_by_id(self, node_id):
        """Obtiene información de un nodo específico"""
        nodes = self.get_available_nodes()
        for node in nodes:
            if node['node_id'] == node_id:
                return node
        return None
    
    def invalidate_cache(self):
        """Invalida el caché de nodos (útil cuando hay cambios)"""
        with self.cache_lock:
            self.cache_time = None
            self.nodes_cache = []
            print("[LOAD BALANCER] Caché invalidado")
    
    @staticmethod
    def _format_bytes(bytes_size):
        """Formatea bytes a formato legible"""
        if bytes_size < 1024:
            return f"{bytes_size}B"
        elif bytes_size < 1024**2:
            return f"{bytes_size/1024:.2f}KB"
        elif bytes_size < 1024**3:
            return f"{bytes_size/(1024**2):.2f}MB"
        else:
            return f"{bytes_size/(1024**3):.2f}GB"