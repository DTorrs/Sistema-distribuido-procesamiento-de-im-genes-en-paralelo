# Archivo: server/session_manager.py
# Gestor de sesiones para el servidor SOAP

import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional

class SessionManager:
    """
    Gestor de sesiones simple en memoria
    
    En producción esto debería estar en Redis o base de datos,
    pero para desarrollo esto funciona perfectamente.
    """
    
    def __init__(self, expiration_hours=24):
        self.sessions: Dict[str, dict] = {}
        self.expiration_hours = expiration_hours
    
    def create_session(self, user_id: int, username: str) -> str:
        """
        Crear nueva sesión
        
        Args:
            user_id: ID del usuario
            username: Username del usuario
            
        Returns:
            session_token: Token único de sesión
        """
        session_token = str(uuid.uuid4()) # Generar token único
        expiration = datetime.now() + timedelta(hours=self.expiration_hours)
        
        self.sessions[session_token] = { #guarda en servidor
            'user_id': user_id,
            'username': username,
            'created_at': datetime.now(),
            'expires_at': expiration
        }
        
        print(f"[SESSION] Sesión creada para {username} (token: {session_token[:8]}...)")
        return session_token
    
    def validate_session(self, session_token: str) -> Optional[dict]:
        """
        Validar sesión
        
        Args:
            session_token: Token a validar
            
        Returns:
            dict con user_id y username si es válida, None si no
        """
        if session_token not in self.sessions: #valida si token existe
            print(f"[SESSION] Token inválido: {session_token[:8]}...")
            return None
        
        session = self.sessions[session_token]
        
        # Verificar expiración
        if datetime.now() > session['expires_at']:
            print(f"[SESSION] Token expirado: {session_token[:8]}...")
            del self.sessions[session_token]
            return None
        
        return {
            'user_id': session['user_id'],
            'username': session['username']
        }
    
    def destroy_session(self, session_token: str) -> bool:
        """
        Destruir sesión (logout)
        
        Args:
            session_token: Token a destruir
            
        Returns:
            True si se destruyó, False si no existía
        """
        if session_token in self.sessions:
            username = self.sessions[session_token]['username']
            del self.sessions[session_token]
            print(f"[SESSION] Sesión destruida para {username}")
            return True
        return False
    
    def cleanup_expired(self):
        """Limpiar sesiones expiradas"""
        now = datetime.now()
        expired = [token for token, data in self.sessions.items() 
                  if now > data['expires_at']]
        
        for token in expired:
            del self.sessions[token]
        
        if expired:
            print(f"[SESSION] {len(expired)} sesiones expiradas limpiadas")