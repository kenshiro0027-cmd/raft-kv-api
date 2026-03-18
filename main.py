import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Raft KV API")

BRIDGE_URL = "http://localhost:8080"

@app.get("/")
def root():
    return {"message": "Raft KV API 起動中"}

@app.get("/cluster/status")
def cluster_status():
    res = httpx.get(f"{BRIDGE_URL}/status")
    return res.json()

@app.post("/store/{key}")
def set_value(key: str, value: str):
    res = httpx.get(f"{BRIDGE_URL}/put", params={"key": key, "value": value})
    if res.status_code != 200:
        raise HTTPException(status_code=500, detail=res.text)
    return res.json()

@app.get("/store/{key}")
def get_value(key: str):
    res = httpx.get(f"{BRIDGE_URL}/get", params={"key": key})
    if res.status_code != 200:
        raise HTTPException(status_code=500, detail=res.text)
    return res.json()

@app.get("/ai/summarize")
def ai_summarize():
    # Raftの全データを取得
    res = httpx.get(f"{BRIDGE_URL}/get", params={"key": ""})
    if res.status_code != 200:
        raise HTTPException(status_code=500, detail=res.text)

    data = res.json()
    all_data = data.get("All", {})

    if not all_data:
        return {"summary": "保存されているデータがありません"}

    # データをテキストに変換
    data_text = "\n".join([f"{k}: {v}" for k, v in all_data.items()])

    # ダミーの要約（本番ではClaude APIを呼ぶ）
    return {
        "data": all_data,
        "summary": f"保存されているデータは{len(all_data)}件です。内容：{data_text}",
        "note": "本番環境ではClaude APIで要約します"
    }