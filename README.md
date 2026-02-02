# sql_conversion_ai

## Qwen SQL API

`api.py` exposes a FastAPI endpoint that wraps the model call from `qwencoder.py`.

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

Example request:

```bash
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{"question":"SELECT DECODE('\"'\"'A'\"'\"','\"'\"'A'\"'\"','\"'\"'1'\"'\"','\"'\"'2'\"'\"') FROM DUAL"}'
```
