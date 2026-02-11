# sql_conversion_ai

## API

`api.py` exposes a FastAPI endpoint that wraps the model call from `qwencoder.py`.

```bash
nohup uvicorn api:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
```

streamlit
```bash
nohup streamlit run app.py \
  --server.address 0.0.0.0 \
  --server.port 8501 \
  --server.headless true \
  > streamlit.log 2>&1 &
```

Example request:

```bash
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{"question":"SELECT DECODE('A','A','1','2') FROM DUAL"}'
```

Optional prompts can also be provided. If omitted, the API keeps the current default prompt behavior.

```bash
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{
    "question":"SELECT * FROM EMP",
    "system_prompt":"You are a SQL expert.",
    "user_prompt":"Convert this Oracle SQL to PostgreSQL syntax only."
  }'
```
