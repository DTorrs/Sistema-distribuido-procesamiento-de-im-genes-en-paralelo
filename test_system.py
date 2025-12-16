import requests
import json
import base64
import os

# Configuración
BASE_URL = "http://localhost:3000/api"
IMAGES_FOLDER = "imagenes"  # Carpeta donde están las imágenes

# Colores para output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_step(step, message):
    print(f"\n{Colors.BLUE}[PASO {step}]{Colors.END} {message}")

def print_success(message):
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")

def print_error(message):
    print(f"{Colors.RED}✗ {message}{Colors.END}")

def print_info(message):
    print(f"{Colors.YELLOW}ℹ {message}{Colors.END}")

def load_images():
    """
    Carga las imágenes 1.jpg - 10.jpg
    
    Returns:
        list: Lista de tuplas (filename, image_base64, file_size)
    """
    print_info(f"Buscando imágenes en carpeta '{IMAGES_FOLDER}/'...")
    
    # Verificar si existe la carpeta
    if not os.path.exists(IMAGES_FOLDER):
        print_error(f"Carpeta '{IMAGES_FOLDER}/' no existe")
        print_error("Por favor, coloca las imágenes 1.jpg - 10.jpg en la carpeta 'imagenes/'")
        return []
    
    images = []
    missing = []
    
    for i in range(1, 11):
        filename = f"{i}.jpg"
        filepath = os.path.join(IMAGES_FOLDER, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                image_bytes = f.read()
                image_base64 = base64.b64encode(image_bytes).decode('utf-8')
                file_size = len(image_bytes)
            
            images.append((filename, image_base64, file_size))
            print_info(f"  ✓ {filename} cargada ({file_size/1024:.1f} KB)")
        else:
            missing.append(filename)
            print_error(f"  ✗ {filename} no encontrada")
    
    if missing:
        print_error(f"Imágenes faltantes: {', '.join(missing)}")
        print_error("Por favor, coloca todas las imágenes en la carpeta 'imagenes/'")
        return []
    
    print_success(f"{len(images)} imágenes cargadas correctamente")
    return images

def test_register():
    """Paso 1: Registrar usuario"""
    print_step(1, "REGISTRAR USUARIO")
    
    url = f"{BASE_URL}/register"
    data = {
        "username": "testuser100",
        "password": "password123",
        "email": "test@example100.com",
        "first_name": "Test",
        "last_name": "User"
    }
    
    response = requests.post(url, json=data)
    
    if response.status_code in [200, 201]:
        result = response.json()
        print_success(f"Usuario registrado: ID={result.get('user_id')}")
        return True
    else:
        print_info("Usuario ya existe (esto es normal)")
        return True

def test_login():
    """Paso 2: Login"""
    print_step(2, "LOGIN")
    
    url = f"{BASE_URL}/login"
    data = {
        "username": "testuser100",
        "password": "password123"
    }
    
    response = requests.post(url, json=data)
    
    if response.status_code == 200:
        result = response.json()
        token = result.get('token')
        print_success(f"Login exitoso")
        print_info(f"Token: {token[:20]}...")
        return token
    else:
        print_error(f"Login fallido: {response.text}")
        return None

def test_process_batch(token):
    """Paso 3: Procesar lote de 10 imágenes"""
    print_step(3, "PROCESAR LOTE DE 10 IMÁGENES")
    
    # Cargar imágenes
    loaded_images = load_images()
    
    if len(loaded_images) != 10:
        print_error(f"Se esperaban 10 imágenes pero se cargaron {len(loaded_images)}")
        return None, None
    
    # Preparar transformaciones - TODAS las disponibles (10 transformaciones)
    transformations_sets = [
        # Imagen 1: grayscale (1 transformación)
        [{"transformation_id": 1, "name": "grayscale", "parameters": {}}],
        
        # Imagen 2: resize (1 transformación)
        [{"transformation_id": 2, "name": "resize", "parameters": {"width": 400, "height": 300}}],
        
        # Imagen 3: crop (1 transformación)
        [{"transformation_id": 3, "name": "crop", "parameters": {"x": 100, "y": 100, "width": 500, "height": 400}}],
        
        # Imagen 4: rotate (1 transformación)
        [{"transformation_id": 4, "name": "rotate", "parameters": {"angle": 45}}],
        
        # Imagen 5: flip (1 transformación)
        [{"transformation_id": 5, "name": "flip", "parameters": {"direction": "horizontal"}}],
        
        # Imagen 6: blur (1 transformación)
        [{"transformation_id": 6, "name": "blur", "parameters": {"radius": 5}}],
        
        # Imagen 7: brightness + contrast (2 transformaciones)
        [
            {"transformation_id": 7, "name": "brightness", "parameters": {"factor": 1.3}},
            {"transformation_id": 8, "name": "contrast", "parameters": {"factor": 1.5}}
        ],
        
        # Imagen 8: watermark (1 transformación)
        [{"transformation_id": 9, "name": "watermark", "parameters": {
            "text": "Procesado 2025",
            "position": "bottom-right",
            "opacity": 0.7
        }}],
        
        # Imagen 9: grayscale + resize + rotate (3 transformaciones)
        [
            {"transformation_id": 1, "name": "grayscale", "parameters": {}},
            {"transformation_id": 2, "name": "resize", "parameters": {"width": 640, "height": 480}},
            {"transformation_id": 4, "name": "rotate", "parameters": {"angle": 90}}
        ],
        
        # Imagen 10: brightness + blur + watermark + flip (4 transformaciones)
        [
            {"transformation_id": 7, "name": "brightness", "parameters": {"factor": 1.2}},
            {"transformation_id": 6, "name": "blur", "parameters": {"radius": 2}},
            {"transformation_id": 9, "name": "watermark", "parameters": {
                "text": "Sistema Distribuido",
                "position": "top-left",
                "opacity": 0.5
            }},
            {"transformation_id": 5, "name": "flip", "parameters": {"direction": "vertical"}}
        ]
    ]
    
    # Construir lista de imágenes para el lote
    images_data = []
    total_size = 0
    
    for idx, (filename, image_base64, file_size) in enumerate(loaded_images):
        transformations = transformations_sets[idx % len(transformations_sets)]
        
        images_data.append({
            "filename": filename,
            "image_data_base64": image_base64,
            "transformations": transformations
        })
        
        total_size += file_size
    
    print_info(f"\nTamaño total del lote: {total_size/1024:.1f} KB ({total_size/1024/1024:.2f} MB)")
    
    url = f"{BASE_URL}/process-batch"
    data = {
        "token": token,
        "batch_name": "Lote 1-10",
        "images": images_data
    }
    
    print_info("Enviando lote al servidor...")
    print_info("(Observa la consola del SOAP Server para ver la distribución)")
    
    response = requests.post(url, json=data)
    
    if response.status_code == 200:
        result = response.json()
        print_success("Lote procesado exitosamente")
        print("")
        print("=" * 60)
        print("RESULTADOS DEL PROCESAMIENTO")
        print("=" * 60)
        print_info(f"Batch ID:        {result.get('batch_id')}")
        print_info(f"Total imágenes:  {result.get('total_images')}")
        print_info(f"Procesadas:      {result.get('processed_images')}")
        print_info(f"Fallidas:        {result.get('failed_images')}")
        print_info(f"Tiempo total:    {result.get('processing_time_ms')} ms ({result.get('processing_time_ms')/1000:.2f} seg)")
        print_info(f"Download URL:    {result.get('download_url')}")
        print("=" * 60)
        return result.get('batch_id'), result.get('download_url')
    else:
        print_error(f"Error procesando lote: {response.text}")
        return None, None

def test_download(batch_id, download_url):
    """Paso 4: Descargar ZIP"""
    print_step(4, "DESCARGAR RESULTADOS")
    
    if not download_url:
        print_error("No hay URL de descarga")
        return False
    
    print_info(f"Descargando desde: {download_url}")
    
    response = requests.get(download_url)
    
    if response.status_code == 200:
        output_filename = f"batch_{batch_id}_results.zip"
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        
        print_success(f"ZIP descargado: {output_filename}")
        print_info(f"Tamaño: {len(response.content)/1024:.1f} KB ({len(response.content)/1024/1024:.2f} MB)")
        
        # Mostrar contenido del ZIP
        import zipfile
        try:
            with zipfile.ZipFile(output_filename, 'r') as zip_file:
                print_info("\nContenido del ZIP:")
                for file_info in zip_file.filelist:
                    print_info(f"  - {file_info.filename} ({file_info.file_size/1024:.1f} KB)")
        except:
            pass
        
        return True
    else:
        print_error(f"Error descargando: {response.status_code}")
        return False

def main():
    """Ejecutar prueba completa"""
    print("\n" + "="*60)
    print("PRUEBA COMPLETA DEL SISTEMA")
    print("Procesamiento de imágenes 1.jpg - 10.jpg")
    print("="*60)
    
    # Paso 1: Registrar
    if not test_register():
        return
    
    # Paso 2: Login
    token = test_login()
    if not token:
        return
    
    # Paso 3: Procesar lote
    batch_id, download_url = test_process_batch(token)
    if not batch_id:
        return
    
    # Paso 4: Descargar
    test_download(batch_id, download_url)
    
    # Paso 5: Mostrar métricas
    show_metrics(batch_id)
    
    print("\n" + "="*60)
    print_success("PRUEBA COMPLETA EXITOSA")
    print("="*60)
    print_info("\nArchivos generados:")
    print_info(f"  - {IMAGES_FOLDER}/       (imágenes originales)")
    print_info(f"  - batch_{batch_id}_results.zip (imágenes procesadas)")
    print("="*60 + "\n")

def show_metrics(batch_id):
    """Mostrar métricas del sistema después del procesamiento"""
    from backend_rest.soap_client import SOAPClient
    
    print_step(5, "MÉTRICAS DEL SISTEMA")
    
    try:
        SOAP_URL = "http://localhost:8000/soap"
        client = SOAPClient(SOAP_URL)
        
        # Métricas de nodos
        print("\n" + "-"*60)
        print("📊 MÉTRICAS DE NODOS")
        print("-"*60)
        
        success, nodes_data = client.get_nodes_metrics()
        if success:
            nodes = nodes_data.get('nodes', [])
            print_success(f"Total de nodos activos: {len(nodes)}\n")
            
            for node in nodes:
                print(f"🖥️  {node.get('node_name', 'N/A')}")
                print(f"   Estado:      {node.get('status', 'N/A')}")
                print(f"   Dirección:   {node.get('ip_address', 'N/A')}:{node.get('port', 'N/A')}")
                print(f"   Recursos:    {node.get('cpu_cores', 'N/A')} cores, {node.get('ram_gb', 'N/A')} GB RAM")
                print(f"   Carga:       {node.get('current_load', 'N/A')}/{node.get('max_concurrent_jobs', 'N/A')}")
                print(f"   Heartbeat:   {node.get('last_heartbeat', 'N/A')}")
                print()
        else:
            print_error("No se pudieron obtener métricas de nodos")
        
        # Métricas del lote procesado
        print("-"*60)
        print(f"📦 MÉTRICAS DEL LOTE {batch_id}")
        print("-"*60)
        
        success, batch_data = client.get_batch_metrics(batch_id)
        if success:
            print_success("Métricas obtenidas:\n")
            print(f"   Nombre:           {batch_data.get('batch_name', 'N/A')}")
            print(f"   Estado:           {batch_data.get('status', 'N/A')}")
            print(f"   Total imágenes:   {batch_data.get('total_images', 0)}")
            print(f"   Procesadas:       {batch_data.get('processed_images', 0)}")
            
            total_time = batch_data.get('total_time_ms', 0)
            if total_time > 0:
                print(f"   Tiempo total:     {total_time} ms ({total_time/1000:.2f} seg)")
                avg_time = total_time / batch_data.get('total_images', 1)
                print(f"   Promedio/imagen:  {avg_time:.0f} ms")
            
            print(f"   Creado:           {batch_data.get('created_at', 'N/A')}")
            if batch_data.get('completed_at'):
                print(f"   Completado:       {batch_data.get('completed_at', 'N/A')}")
            
            # Distribución por nodo
            node_dist = batch_data.get('node_distribution', [])
            if node_dist:
                print(f"\n   📊 Distribución por nodo:")
                for dist in node_dist:
                    node_name = dist.get('node_name', 'N/A')
                    images_proc = dist.get('images_processed', 0)
                    print(f"      • {node_name}: {images_proc} imágenes")
        else:
            print_error("No se pudieron obtener métricas del lote")
        
        print("-"*60)
        
    except Exception as e:
        print_error(f"Error obteniendo métricas: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
