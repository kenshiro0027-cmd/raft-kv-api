# Raft KV API

分散合意アルゴリズム **Raft** をベースにした分散KVストアを、**FastAPI** でREST APIとして公開したプロジェクトです。

## 概要

大学の研究（分散共同編集基盤へのアクセス制御導入）で実装したRaftクラスタを、実際のAPIサービスとして応用しました。

## システム構成
```
クライアント（ブラウザ）
  ↓ HTTP
FastAPI（Python） ← このリポジトリ
  ↓ HTTP
Bridgeサーバー（Go）
  ↓ RPC
Raftクラスタ（Go・3ノード）
```

## 技術選定の理由

- **FastAPI**：自動でSwagger UIが生成され、APIドキュメントが不要になるため
- **Bridge構成**：GoのRPCとPythonのHTTPの言語の壁を越えるため
- **Raft**：リーダー選出・ログ複製により、1ノード障害時もデータが失われない

## エンドポイント

| メソッド | パス | 説明 |
|---|---|---|
| GET | `/` | 起動確認 |
| GET | `/cluster/status` | Raftクラスタの状態確認 |
| POST | `/store/{key}` | データをRaftクラスタに保存 |
| GET | `/store/{key}` | データをRaftクラスタから取得 |
| GET | `/ai/summarize` | 保存データの要約（Claude API連携予定） |

## 起動方法

**1. Raftクラスタを起動（ターミナル3つ）**
```bash
go run main.go --id=node1 --raft-addr=127.0.0.1:7001 --rpc=127.0.0.1:9101 --data-dir=./cluster --bootstrap
go run main.go --id=node2 --raft-addr=127.0.0.1:7002 --rpc=127.0.0.1:9102 --data-dir=./cluster
go run main.go --id=node3 --raft-addr=127.0.0.1:7003 --rpc=127.0.0.1:9103 --data-dir=./cluster
```

**2. Bridgeサーバーを起動**
```bash
cd bridge
go run bridge.go
```

**3. FastAPIを起動**
```bash
pip install fastapi uvicorn httpx
python -m uvicorn main:app --reload
```

**4. ブラウザでアクセス**
```
http://localhost:8000/docs
```

## 研究背景

本プロジェクトは、法政大学基盤ソフトウェア研究室での研究「分散共同編集基盤DSONへのRaftを介した細粒度アクセス制御の導入」の実装経験をベースにしています。
