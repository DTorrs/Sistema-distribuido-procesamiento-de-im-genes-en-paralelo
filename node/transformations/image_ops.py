# Archivo: node/transformations/image_ops.py
# MOTOR DE PROCESAMIENTO DE IMÁGENES - COMPLETO

from PIL import Image, ImageOps, ImageEnhance, ImageFilter, ImageDraw, ImageFont
import json
import os
import time

class ImageProcessor:
    """Clase central del procesamiento de imágenes con todas las transformaciones"""
    
    @staticmethod
    def process_image(image_path, transformations, output_dir="output"):
        """
        Procesa una imagen con las transformaciones especificadas
        
        Args:
            image_path: Ruta al archivo de imagen
            transformations: Lista de transformaciones (máximo 5)
            output_dir: Directorio de salida
            
        Returns:
            dict con success, result_path, error_message, processing_time_ms
        """
        start_time = time.time()
        
        try:
            # Validar número de transformaciones
            if len(transformations) > 5:
                return {
                    'success': False,
                    'result_path': '',
                    'error_message': 'Máximo 5 transformaciones permitidas',
                    'processing_time_ms': 0
                }
            
            # Crear directorio de salida
            os.makedirs(output_dir, exist_ok=True)
            
            # Cargar imagen
            print(f"[PROCESADOR] Cargando imagen: {image_path}")
            img = Image.open(image_path)
            original_format = img.format
            print(f"[PROCESADOR] Imagen cargada: {img.width}x{img.height} ({original_format})")
            
            # Preparar nombre de archivo
            original_filename = os.path.basename(image_path)
            filename_parts = os.path.splitext(original_filename)
            
            # Aplicar transformaciones secuencialmente
            for i, transform in enumerate(transformations):
                name = transform.get('name', '')
                params = json.loads(transform.get('parameters', '{}')) if isinstance(transform.get('parameters'), str) else transform.get('parameters', {})
                
                print(f"[PROCESADOR] Aplicando transformación {i+1}/{len(transformations)}: {name}")
                
                try:
#######################################################################################################################################################################################################                    
                    img = ImageProcessor._apply_transformation(img, name, params)
#################################################################################################################################################################################################################
                except Exception as e:
                    print(f"[PROCESADOR] Error en transformación {name}: {e}")
                    return {
                        'success': False,
                        'result_path': '',
                        'error_message': f'Error en transformación {name}: {str(e)}',
                        'processing_time_ms': int((time.time() - start_time) * 1000)
                    }
            
            # Guardar resultado
            result_filename = f"{filename_parts[0]}_processed{filename_parts[1]}"
            result_path = os.path.join(output_dir, result_filename)
            
            # IMPORTANTE: JPEG no soporta RGBA, convertir a RGB si es necesario
            if img.mode in ['RGBA', 'LA', 'P'] and (original_format == 'JPEG' or filename_parts[1].lower() in ['.jpg', '.jpeg']):
                print(f"[PROCESADOR] Convirtiendo {img.mode} a RGB para JPEG")
                # Crear fondo blanco
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'RGBA':
                    background.paste(img, mask=img.split()[3])  # Usar canal alpha como máscara
                else:
                    background.paste(img)
                img = background
            
            # Si hay conversión de formato, aplicarla al guardar
            if img.format:
                img.save(result_path)
            else:
                img.save(result_path, format=original_format)
            
            print(f"[PROCESADOR] Resultado guardado en: {result_path}")
            
            processing_time = int((time.time() - start_time) * 1000)
            print(f"[PROCESADOR] Procesamiento completado en {processing_time} ms")
            
            return {
                'success': True,
                'result_path': result_path,
                'error_message': '',
                'processing_time_ms': processing_time
            }
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            print(f"[PROCESADOR] ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'result_path': '',
                'error_message': str(e),
                'processing_time_ms': processing_time
            }
    
    @staticmethod
    def _apply_transformation(img, name, params):
        """Aplica una transformación específica a la imagen"""
        
        if name == 'grayscale':
            print(f"[PROCESADOR] → Convirtiendo a escala de grises")
            return ImageOps.grayscale(img)
        
        elif name == 'resize':
            width = params.get('width', img.width)
            height = params.get('height', img.height)
            keep_aspect = params.get('keep_aspect_ratio', True)
            
            print(f"[PROCESADOR] → Redimensionando a {width}x{height} (mantener aspecto: {keep_aspect})")
            
            if keep_aspect:
                img = ImageOps.contain(img, (width, height))
            else:
                img = img.resize((width, height))
            
            print(f"[PROCESADOR] → Nueva dimensión: {img.width}x{img.height}")
            return img
        
        elif name == 'crop':
            x = params.get('x', 0)
            y = params.get('y', 0)
            width = params.get('width', img.width)
            height = params.get('height', img.height)
            
            print(f"[PROCESADOR] → Recortando región: ({x},{y}) {width}x{height}")
            return img.crop((x, y, x + width, y + height))
        
        elif name == 'rotate':
            angle = params.get('angle', 0)
            expand = params.get('expand', True)
            
            print(f"[PROCESADOR] → Rotando {angle} grados")
            return img.rotate(angle, expand=expand)
        
        elif name == 'flip':
            direction = params.get('direction', 'horizontal').lower()
            
            print(f"[PROCESADOR] → Reflejando ({direction})")
            
            if direction == 'horizontal':
                return ImageOps.mirror(img)
            elif direction == 'vertical':
                return ImageOps.flip(img)
            else:
                print(f"[PROCESADOR] ⚠ Dirección inválida, usando horizontal")
                return ImageOps.mirror(img)
        
        elif name == 'blur':
            radius = params.get('radius', 2)
            
            print(f"[PROCESADOR] → Desenfocando (radio: {radius})")
            return img.filter(ImageFilter.GaussianBlur(radius=radius))
        
        elif name == 'brightness':
            factor = params.get('factor', 1.0)
            
            print(f"[PROCESADOR] → Ajustando brillo (factor: {factor})")
            enhancer = ImageEnhance.Brightness(img)
            return enhancer.enhance(factor)
        
        elif name == 'contrast':
            factor = params.get('factor', 1.0)
            
            print(f"[PROCESADOR] → Ajustando contraste (factor: {factor})")
            enhancer = ImageEnhance.Contrast(img)
            return enhancer.enhance(factor)
        
        elif name == 'watermark':
            text = params.get('text', 'Watermark')
            position = params.get('position', 'bottom-right').lower()
            opacity = params.get('opacity', 0.5)
            
            print(f"[PROCESADOR] → Insertando marca de agua: '{text}' ({position})")
            
            # Convertir a RGBA si no lo es
            if img.mode != 'RGBA':
                img = img.convert('RGBA')
            
            # Crear capa transparente
            txt_layer = Image.new('RGBA', img.size, (255, 255, 255, 0))
            draw = ImageDraw.Draw(txt_layer)
            
            # Usar fuente por defecto
            try:
                font = ImageFont.truetype("arial.ttf", 36)
            except:
                font = ImageFont.load_default()
            
            # Calcular posición
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            if position == 'top-left':
                pos = (10, 10)
            elif position == 'top-right':
                pos = (img.width - text_width - 10, 10)
            elif position == 'bottom-left':
                pos = (10, img.height - text_height - 10)
            else:  # bottom-right (default)
                pos = (img.width - text_width - 10, img.height - text_height - 10)
            
            # Dibujar texto con opacidad
            alpha = int(255 * opacity)
            draw.text(pos, text, fill=(255, 255, 255, alpha), font=font)
            
            # Combinar capas
            watermarked = Image.alpha_composite(img, txt_layer)
            
            return watermarked
        
        elif name == 'format_conversion':
            target_format = params.get('target_format', 'PNG').upper()
            
            print(f"[PROCESADOR] → Convirtiendo formato a {target_format}")
            
            # Validar formato
            if target_format not in ['JPEG', 'JPG', 'PNG', 'TIF', 'TIFF']:
                print(f"[PROCESADOR] ⚠ Formato inválido, usando PNG")
                target_format = 'PNG'
            
            # JPEG no soporta transparencia
            if target_format in ['JPEG', 'JPG'] and img.mode in ['RGBA', 'LA']:
                print(f"[PROCESADOR] → Convirtiendo a RGB para JPEG")
                # Crear fondo blanco
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'RGBA':
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img)
                img = background
            
            # Configurar formato para guardado
            img.format = target_format if target_format != 'JPG' else 'JPEG'
            
            return img
        
        else:
            print(f"[PROCESADOR] ⚠ Transformación desconocida: {name}")
            return img