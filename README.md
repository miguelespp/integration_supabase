# Integración con Supabase y PostgreSQL — manejo de variables de entorno

Proyecto con dos puntos de entrada:
- `app.py`: cliente de Supabase
- `preapp.py`: extracción directa de esquema de PostgreSQL

## Configuración rápida

1. **Crear archivo `.env` local** (no subir a git):
   - Copia `.env.sample` a `.env` y completa las variables según tu uso.

2. **Instalar dependencias** (recomendado en virtualenv):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

### Para Supabase (`app.py`)

Variables requeridas: `SUPABASE_URL`, `SUPABASE_KEY`

```bash
python app.py
```

### Para PostgreSQL directo (`preapp.py`)

Variables requeridas: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`

```bash
python preapp.py
```

Este script conecta a PostgreSQL y extrae información de todas las tablas del esquema `public`, mostrando nombre y tipo de cada columna.

## Notas técnicas

- **Desarrollo**: `python-dotenv` facilita cargar `.env` automáticamente
- **Producción**: usar gestor de secretos del proveedor (variables de entorno de Docker, CI/CD secrets, etc.)
- El código funciona sin `python-dotenv`; solo requiere que las variables estén en el entorno del sistema