# データバリデーションツール 設計書

## 目次

1. [概要](#1-概要)
2. [アーキテクチャ](#2-アーキテクチャ)
3. [ディレクトリ構成](#3-ディレクトリ構成)
4. [依存ライブラリ](#4-依存ライブラリ)
5. [設定ファイル仕様](#5-設定ファイル仕様)
6. [スキーマYAML仕様](#6-スキーマyaml仕様)
7. [データモデル仕様](#7-データモデル仕様)
8. [モジュール仕様](#8-モジュール仕様)
9. [セキュリティ仕様](#9-セキュリティ仕様)
10. [エラーハンドリング仕様](#10-エラーハンドリング仕様)
11. [HTMLレポート仕様](#11-htmlレポート仕様)
12. [実装順序](#12-実装順序)
13. [制約・既知の限界](#13-制約既知の限界)

---

## 1. 概要

### 目的

分析前の受領データ（CSV / XLSX / Parquet）が、事前に定義したテーブル定義書（YAML）と一致しているかを自動バリデーションするPoCツール。

### 解決する問題

- 分析者ごとにバリデーション品質が異なる（属人化）
- バリデーション作業に時間がかかる

### スコープ

| 対象 | 内容 |
|---|---|
| 入力データ形式 | CSV, XLSX（`.xlsx`のみ。`.xls`は非対応）, Parquet |
| データベースエンジン | DuckDB |
| 定義書フォーマット | YAML（Pydanticでスキーマ検証済み） |
| 出力 | HTMLレポート |
| 使用者 | 社内分析者（内部ツール） |

---

## 2. アーキテクチャ

### 処理フロー

```
[config.yaml]          [schema/*.yaml]       [relations.yaml（任意）]
      │                      │                        │
      │              [Pydanticバリデーション]          │
      │                      │                        │
      └──────────┬───────────┘                        │
                 │                                    │
         [DAGによるロード順決定]                       │
         （外部キー依存関係を解決）                    │
                 │                                    │
         [DuckDBテーブルをCREATE]                      │
         （PK / UNIQUE / FK / NOT NULL 制約付き）      │
                 │                                    │
         [ファイルを1件ずつINSERT]                     │
         （エラーをキャッチ・パース）                  │
                 │                                    │
         [ロジックバリデーション実行]                  │
         （checks / aggregation_checks）              │
                 │                                    │
         [リレーションカーディナリティ検証]────────────┘
         （一意性・参照整合性チェック）
                 │
         [基本統計量算出]
         （共通統計: 一括SQL / 数値型追加統計: 列ごと個別SQL）
                 │
         [Parquetエクスポート（任意）]
         （テーブル＋リレーション結果でゲート）
                 │
         [HTMLレポート生成]
```

### 設計原則

- **フェイルファスト**: YAMLのスキーマ違反・循環依存はツール起動時に即時例外
- **1ファイル1トランザクション**: ファイル単位でINSERTしエラーをファイル単位で特定する
- **DBへの委譲**: 構造チェック（型・NULL・PK・FK・UNIQUE）はDuckDBの制約機能に委譲し、自前で再実装しない
- **raw_messageの保持**: DuckDBエラーのパースが失敗しても生メッセージをレポートに出力する

---

## 3. ディレクトリ構成

### リポジトリ構成

```
tval/                        # リポジトリルート
├── pyproject.toml
├── README.md
├── DESIGN.md
├── src/
│   └── tval/                # Pythonパッケージ本体
│       ├── __init__.py
│       ├── cli.py           # CLIエントリポイント（argparse）
│       ├── init.py          # tval initコマンドのロジック
│       ├── main.py          # tval runコマンドのロジック
│       ├── parser.py        # YAMLパース・Pydanticモデル定義
│       ├── builder.py       # CREATE TABLE生成・DAGロード順決定
│       ├── loader.py        # ファイルINSERT・DuckDBエラーパース
│       ├── checker.py       # ロジックバリデーション実行
│       ├── relation.py      # テーブル間リレーションカーディナリティ検証
│       ├── profiler.py      # 基本統計量算出
│       ├── exporter.py      # Parquetエクスポート
│       ├── reporter.py      # HTMLレポート生成
│       ├── logger.py        # 構造化JSONロガー
│       └── templates/
│           └── report.html.j2
└── tests/
```

### 分析リポジトリ内でのtval利用時の構成

`tval init`実行後、分析リポジトリ内に以下が生成される。

```
your-analysis-repo/
├── tval/                    # tval init で生成
│   ├── config.yaml
│   ├── schema/              # テーブル定義YAML置き場
│   │   ├── orders.yaml
│   │   └── users.yaml
│   ├── data/                # 受領データ置き場（.gitignore対象）
│   │   ├── orders/
│   │   └── users/
│   └── output/              # レポート出力先（.gitignore対象）
│       └── report.html
├── notebooks/
├── src/
└── .gitignore               # tval/data/, tval/output/ が追記される
```

---

## 4. 依存ライブラリ

```
duckdb
pydantic
pyyaml
jinja2
chardet
```

**注意事項**

- Pythonバージョン: **3.9以上**必須（`graphlib`標準ライブラリ使用のため）
- XLSX読み込みはDuckDBの`read_xlsx`をネイティブ使用。`openpyxl`は不要
- `.xls`（旧Excel形式）は非対応。受領時に`.xlsx`への変換を運用ルールとする
- ロギングはPython標準`logging`モジュール＋カスタムJSONフォーマッターで実装。外部ライブラリ追加なし

---

## 4.1 ロギング仕様

### 出力形式

1行1JSONで標準エラー出力（stderr）に出力する。

```json
{"timestamp": "2024-01-01T12:00:00.000000", "level": "INFO", "module": "loader", "message": "ファイルロード開始", "table": "orders", "file": "./data/orders/jan.csv"}
```

### フォーマッター実装

`validator/logger.py` として実装する。

```python
import json
import logging
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }
        # extra引数で渡された任意フィールドを展開
        for key, value in record.__dict__.items():
            if key not in {
                "timestamp", "level", "module", "message",
                "name", "msg", "args", "created", "filename",
                "funcName", "levelname", "levelno", "lineno",
                "module", "msecs", "pathname", "process",
                "processName", "relativeCreated", "stack_info",
                "thread", "threadName", "exc_info", "exc_text",
            }:
                log[key] = value
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log, ensure_ascii=False)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
```

### 使用方法

各モジュールで以下のように使用する。

```python
from .logger import get_logger
logger = get_logger(__name__)

# 基本
logger.info("テーブル作成", extra={"table": "orders"})

# エラー（例外情報を含む）
logger.error("ファイルロード失敗", extra={"file": file_path, "table": table_name}, exc_info=True)
```

### 各モジュールのログ出力要件

| モジュール | レベル | ログを出すタイミング | 必須extraフィールド |
|---|---|---|---|
| `main.py` | INFO | ツール起動時・終了時 | - |
| `builder.py` | INFO | テーブル作成時 | `table` |
| `loader.py` | INFO | ファイルロード開始・完了時 | `table`, `file` |
| `loader.py` | ERROR | ファイルロードエラー時 | `table`, `file`, `error_type` |
| `loader.py` | WARNING | 非対応拡張子スキップ時 | `file` |
| `checker.py` | INFO | チェック実行開始・完了時 | `table` |
| `checker.py` | WARNING | チェックSKIPPED時 | `table`, `check_description` |
| `checker.py` | ERROR | チェックNG時 | `table`, `check_description` |
| `profiler.py` | INFO | 統計量取得開始・完了時 | `table` |
| `profiler.py` | ERROR | 統計量取得失敗時 | `table`, `column` |

---

## 4.2 パッケージング仕様

### パッケージ名・コマンド名

| 項目 | 値 |
|---|---|
| PyPIパッケージ名 | `tval` |
| CLIコマンド名 | `tval` |
| Pythonパッケージ名 | `tval` |

### `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "tval"
version = "0.1.0"
description = "Table data validator for pre-analysis data validation"
requires-python = ">=3.9"
dependencies = [
    "duckdb",
    "pydantic",
    "pyyaml",
    "jinja2",
    "chardet",
]

[project.optional-dependencies]
dev = [
    "pytest",
    "ruff",
    "mypy",
]

[project.scripts]
tval = "tval.cli:main"

[tool.ruff]
target-version = "py39"
line-length = 88

[tool.ruff.lint]
select = ["E", "F", "I"]   # pycodestyle, pyflakes, isort

[tool.mypy]
python_version = "3.9"
strict = true
```

### リポジトリ構成（パッケージング後）

```
tval/                        # リポジトリルート
├── pyproject.toml
├── README.md
├── src/
│   └── tval/                # Pythonパッケージ
│       ├── __init__.py
│       ├── cli.py           # CLIエントリポイント
│       ├── main.py          # バリデーション実行ロジック
│       ├── parser.py
│       ├── builder.py
│       ├── loader.py
│       ├── checker.py
│       ├── profiler.py
│       ├── reporter.py
│       ├── logger.py
│       └── templates/
│           └── report.html.j2
└── tests/                   # テストコード（任意）
```

`src/`レイアウトを採用する。インストール前に誤ってローカルパッケージが読まれるリスクを防ぐため。

### PyPI公開手順

```bash
pip install build twine
python -m build
twine upload dist/*
```

---

## 4.3 CLI仕様

### サブコマンド構成

```
tval <subcommand> [options]

サブコマンド:
  init    プロジェクトスケルトンを生成する
  run     バリデーションを実行する
```

### `tval init`

**目的**: 分析リポジトリ内にtvalプロジェクトスケルトンを生成する

**使用方法**

```bash
tval init [--dir PATH]
```

| オプション | デフォルト | 説明 |
|---|---|---|
| `--dir` | `./tval` | スケルトンを生成するディレクトリ |

**生成されるディレクトリ・ファイル**

```
{dir}/
├── config.yaml        # テンプレート（database_path・schema_dir・output_pathを記載）
├── schema/            # 空ディレクトリ（.gitkeep配置）
├── data/              # 空ディレクトリ（.gitkeep配置）
└── output/            # 空ディレクトリ（.gitkeep配置）
```

**`.gitignore`への自動追記**

カレントディレクトリの`.gitignore`に以下を追記する。
`.gitignore`が存在しない場合は新規作成する。
すでに同一エントリが存在する場合は追記しない（冪等性を保つ）。

```
# tval
tval/data/
tval/output/
```

**`config.yaml`テンプレート**

```yaml
database_path: ./tval/work.duckdb
schema_dir: ./tval/schema
output_path: ./tval/output/report.html
encoding_confidence_threshold: 0.8
```

**エラー処理**

`{dir}`がすでに存在する場合は処理を中断しエラーメッセージを表示する。上書きは行わない。

**実行例**

```bash
$ tval init
✅ tval/ を作成しました
✅ .gitignore に tval/data/, tval/output/ を追記しました

次のステップ:
  1. tval/schema/ にテーブル定義YAMLを追加してください
  2. tval/data/ に受領データを配置してください
  3. tval run で バリデーションを実行してください
```

---

### `tval run`

**目的**: バリデーションを実行しHTMLレポートを生成する

**使用方法**

```bash
tval run [--config PATH] [--export]
```

| オプション | 説明 |
|---|---|
| `--config` | `config.yaml`のパスを明示指定する |
| `--export` | バリデーション全成功時にParquetファイルを書き出す。1テーブルでもNGがあればエクスポート全体をスキップ |

**`--config`省略時の探索順序**

1. `./tval/config.yaml`
2. `./config.yaml`

どちらも存在しない場合はエラーメッセージを表示して終了する。

**実行例**

```bash
# --config省略（自動探索）
tval run

# --configで明示指定
tval run --config /path/to/project/tval/config.yaml
```

### `cli.py`の実装方針

`argparse`を使用する。外部ライブラリ（`click`等）は使用しない。

```python
import argparse
import sys

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tval",
        description="Table data validator",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # init
    init_parser = subparsers.add_parser("init", help="プロジェクトスケルトンを生成する")
    init_parser.add_argument("--dir", default="./tval", help="生成先ディレクトリ（デフォルト: ./tval）")

    # run
    run_parser = subparsers.add_parser("run", help="バリデーションを実行する")
    run_parser.add_argument("--config", default=None, help="config.yamlのパス")
    run_parser.add_argument("--export", action="store_true", help="バリデーション全成功時にParquetを書き出す")

    args = parser.parse_args()

    if args.subcommand == "init":
        from .init import run_init
        run_init(args.dir)
    elif args.subcommand == "run":
        from .main import run
        run(args.config, export=args.export)

if __name__ == "__main__":
    main()
```

`init`コマンドのロジックは`tval/init.py`として分離する。

---

## 4.1 ロギング仕様

### 出力形式

1行1JSONで標準エラー出力（stderr）に出力する。

```json
{"timestamp": "2024-01-01T12:00:00.000000", "level": "INFO", "module": "loader", "message": "ファイルロード開始", "table": "orders", "file": "./data/orders/jan.csv"}
```

### フォーマッター実装

`validator/logger.py` として実装する。

```python
import json
import logging
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }
        # extra引数で渡された任意フィールドを展開
        for key, value in record.__dict__.items():
            if key not in {
                "timestamp", "level", "module", "message",
                "name", "msg", "args", "created", "filename",
                "funcName", "levelname", "levelno", "lineno",
                "module", "msecs", "pathname", "process",
                "processName", "relativeCreated", "stack_info",
                "thread", "threadName", "exc_info", "exc_text",
            }:
                log[key] = value
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log, ensure_ascii=False)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
```

### 使用方法

各モジュールで以下のように使用する。

```python
from .logger import get_logger
logger = get_logger(__name__)

# 基本
logger.info("テーブル作成", extra={"table": "orders"})

# エラー（例外情報を含む）
logger.error("ファイルロード失敗", extra={"file": file_path, "table": table_name}, exc_info=True)
```

### 各モジュールのログ出力要件

| モジュール | レベル | ログを出すタイミング | 必須extraフィールド |
|---|---|---|---|
| `main.py` | INFO | ツール起動時・終了時 | - |
| `builder.py` | INFO | テーブル作成時 | `table` |
| `loader.py` | INFO | ファイルロード開始・完了時 | `table`, `file` |
| `loader.py` | INFO | CSV文字コードをUTF-8に変換した場合（非UTF-8のみ） | `file`, `detected_encoding`, `confidence` |
| `loader.py` | ERROR | CSV文字コード検出の信頼度が閾値未満の場合 | `file`, `detected_encoding`, `confidence`, `threshold` |
| `loader.py` | ERROR | ファイルロードエラー時 | `table`, `file`, `error_type` |
| `loader.py` | WARNING | 非対応拡張子スキップ時 | `file` |
| `checker.py` | INFO | チェック実行開始・完了時 | `table` |
| `checker.py` | WARNING | チェックSKIPPED時 | `table`, `check_description` |
| `checker.py` | ERROR | チェックNG時 | `table`, `check_description` |
| `profiler.py` | INFO | 統計量取得開始・完了時 | `table` |
| `profiler.py` | ERROR | 統計量取得失敗時 | `table`, `column` |

---

## 5. 設定ファイル仕様

### `config.yaml`

```yaml
database_path: ./tval/work.duckdb
schema_dir: ./tval/schema
output_path: ./tval/output/report.html
encoding_confidence_threshold: 0.8   # chardet信頼度の閾値（0.0〜1.0）。省略時は0.8
# relations_path: ./tval/relations.yaml  # リレーション定義（任意）
```

| キー | 型 | 必須 | 説明 |
|---|---|---|---|
| `database_path` | string | ✅ | DuckDBファイルパス。実行のたびに既存ファイルを削除して再作成する |
| `schema_dir` | string | ✅ | `*.yaml`を格納したディレクトリ。サブディレクトリは読まない |
| `output_path` | string | ✅ | レポートHTML出力先。親ディレクトリが存在しない場合は作成する |
| `encoding_confidence_threshold` | float | ❌ | chardetの信頼度閾値（0.0〜1.0）。省略時は`0.8`。この値未満の場合はLoadErrorとして記録しロードをスキップする |
| `relations_path` | string | ❌ | テーブル間リレーション定義ファイル（`relations.yaml`）のパス。省略時はリレーション検証をスキップ |

---

## 6. スキーマYAML仕様

1テーブルにつき1ファイル。`schema_dir`以下に配置する。

### 完全な例

```yaml
table:
  name: orders
  description: 注文テーブル
  source_dir: ./data/orders/    # このディレクトリ以下の対応拡張子ファイルを全件ロード

columns:
  - name: order_id
    logical_name: 注文ID
    type: INTEGER
    not_null: true
    description: システム採番の注文識別子    # optional

  - name: user_id
    logical_name: ユーザーID
    type: INTEGER
    not_null: true

  - name: status
    logical_name: ステータス
    type: VARCHAR
    not_null: true
    description: pending / shipped / cancelled のいずれか  # optional
    allowed_values: ["pending", "shipped", "cancelled"]   # optional。文字列型の許容値リスト

  - name: sub_total
    logical_name: 小計
    type: DOUBLE
    not_null: true

  - name: tax
    logical_name: 消費税
    type: DOUBLE
    not_null: true

  - name: total
    logical_name: 合計金額
    type: DOUBLE
    not_null: true

  - name: created_at
    logical_name: 作成日時
    type: TIMESTAMP
    not_null: true
    format: "%Y/%m/%d %H:%M:%S"   # optional。DATE/TIMESTAMP/TIME型のみ有効

table_constraints:
  primary_key:
    columns: [order_id]             # 複合PKの場合は複数要素を列挙

  unique:
    - columns: [order_id]           # 単一列UNIQUE
    - columns: [user_id, created_at]  # 複合UNIQUE

  foreign_keys:
    - columns: [user_id]            # 自テーブル側カラム（複合FK対応）
      references:
        table: users                # 参照先テーブル名（同一schema_dir内に定義が必要）
        columns: [user_id]          # 参照先カラム（複合FK対応）

  checks:
    - description: 合計は小計+税と一致すること
      query: "SELECT COUNT(*) FROM {table} WHERE total != sub_total + tax"
      expect_zero: true

  aggregation_checks:
    - description: pendingステータスの比率は50%未満であること
      query: |
        SELECT COUNT(*) FROM (
          SELECT 1 FROM {table}
          HAVING SUM(CASE WHEN status = 'pending' THEN 1.0 ELSE 0 END) / COUNT(*) >= 0.5
        )
      expect_zero: true

export:                             # optional。省略時はエクスポートしない
  partition_by: [created_at]        # optional。パーティション列リスト。省略時はパーティションなし

# 制約が何もない場合は全フィールドを空配列で明示する（table_constraintsは必須）
# table_constraints:
#   primary_key: []
#   foreign_keys: []
#   unique: []
#   checks: []
#   aggregation_checks: []
```

### フィールド仕様

#### `table`（必須）

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | テーブル物理名。識別子として使用（英数字・アンダースコアのみ） |
| `description` | string | ✅ | テーブルの概要説明 |
| `source_dir` | string | ✅ | 受領データが格納されたディレクトリパス。YAML読み込み時点で存在必須 |

#### `columns`（必須・1件以上）

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | カラム物理名 |
| `logical_name` | string | ✅ | カラム論理名（レポート表示用） |
| `type` | string | ✅ | DuckDB型名。パース時に自動で大文字化される。`VARCHAR(N)`・`CHAR(N)`・`DECIMAL(p,s)`のように精度・長さ指定を含む型を記述することで、DuckDBがINSERT時に自動で制約として検証する。例: `VARCHAR(8)`（8文字以内）、`DECIMAL(10,2)`（整数部8桁・小数部2桁） |
| `not_null` | boolean | ✅ | `true`でNOT NULL制約を付与 |
| `description` | string | ❌ | カラムの補足説明（レポート表示用）。省略時は空文字列として扱う |
| `allowed_values` | string[] | ❌ | 許容する値のリスト。省略時はチェックなし。`VARCHAR`等の文字列型カラムを想定。ロード後に`checker.py`が自動でSQLに変換して検証する |
| `format` | string | ❌ | 日付・日時・時刻フォーマット文字列（例: `"%Y/%m/%d"`）。`DATE`・`TIMESTAMP`・`TIME`型のカラムにのみ有効。省略時はDuckDBの自動推論に委ねる。他の型に指定した場合はYAML読み込み時にValidationError |

#### `table_constraints`（**必須**）

制約がない場合も省略不可。全フィールドを空配列で明示すること。

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `primary_key` | array | ✅ | `columns`: string[]。複合PK対応。制約なしの場合は`[]` |
| `unique` | array | ✅ | `columns`: string[]の配列。複合UNIQUE対応。制約なしの場合は`[]` |
| `foreign_keys` | array | ✅ | 後述。複合FK対応。制約なしの場合は`[]` |
| `checks` | array | ✅ | 列間・条件チェック。後述。制約なしの場合は`[]` |
| `aggregation_checks` | array | ✅ | 集計レベルチェック。後述。実装上`checks`と同一仕組み。制約なしの場合は`[]` |

#### `foreign_keys[*]`

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `columns` | string[] | ✅ | 自テーブル側カラム名リスト |
| `references.table` | string | ✅ | 参照先テーブル名 |
| `references.columns` | string[] | ✅ | 参照先カラム名リスト |

#### `export`（任意）

省略時はエクスポートを行わない。`tval run --export`フラグと組み合わせて使用する。

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `partition_by` | string[] | ❌ | パーティション列リスト。省略時はパーティションなし。指定列はそのテーブルの`columns`に存在しなければならない |

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `description` | string | ✅ | チェック内容の説明（レポート表示用） |
| `query` | string | ✅ | 実行するSQL。`{table}`がテーブル物理名に置換される |
| `expect_zero` | boolean | ❌ | デフォルト`true`。クエリ結果が0件であればOK、1件以上でNG |

**`query`の記述ルール**

- `{table}`プレースホルダを使用してテーブル名を参照すること
- クエリは`SELECT COUNT(*) FROM ...`の形式を推奨
- 内部ツールのため任意SQLを許容する（インジェクション対策の対象外）

---

## 6.1 リレーションYAML仕様（`relations.yaml`）

テーブル間のカーディナリティ（1:1, 1:N, N:1, N:N）を定義するオプショナルなファイル。`config.yaml`の`relations_path`で参照する。

### 完全な例

```yaml
relations:
  - name: users-orders
    cardinality: "1:N"
    from:
      table: users
      columns: [user_id]
    to:
      table: orders
      columns: [user_id]

  - name: users-profiles
    cardinality: "1:1"
    from:
      table: users
      columns: [user_id]
    to:
      table: profiles
      columns: [user_id]
```

### フィールド定義

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | リレーション名（レポート表示用） |
| `cardinality` | string | ✅ | `"1:1"`, `"1:N"`, `"N:1"`, `"N:N"` のいずれか |
| `from.table` | string | ✅ | from側テーブル名（`schema_dir`に定義が必要） |
| `from.columns` | string[] | ✅ | from側のカラム名リスト |
| `to.table` | string | ✅ | to側テーブル名（`schema_dir`に定義が必要） |
| `to.columns` | string[] | ✅ | to側のカラム名リスト |

### カーディナリティ別の検証内容

| カーディナリティ | 検証内容 | チェック数 |
|---|---|---|
| `1:1` | from側一意性 + to側一意性 + from→to参照整合性 + to→from参照整合性 | 4 |
| `1:N` | from側（1側）一意性 + to→from参照整合性 | 2 |
| `N:1` | to側（1側）一意性 + from→to参照整合性 | 2 |
| `N:N` | from→to参照整合性 + to→from参照整合性 | 2 |

### 検証SQL

**一意性チェック**: キー列の組み合わせに重複がないことを確認
```sql
SELECT COUNT(*) FROM (
  SELECT col1, col2 FROM table GROUP BY col1, col2 HAVING COUNT(*) > 1
)
```

**参照整合性チェック**: source側の値がtarget側に存在することを確認（NULLは除外）
```sql
SELECT COUNT(*) FROM source s
LEFT JOIN target t ON s.col = t.col
WHERE s.col IS NOT NULL AND t.col IS NULL
```

### 注意事項

- `from`はPython予約語のため、内部ではYAML読込時に`from` → `from_`にリネームして処理する
- NULL値は参照整合性チェックから除外される（SQL FK標準に準拠）
- いずれかのテーブルにロードエラーがある場合、そのリレーションの全チェックは`SKIPPED`となる
- リレーション検証結果はテーブル別セクションではなく、独立した「リレーションカーディナリティ検証」セクションとしてレポートに表示される

---

## 7. データモデル仕様

`validator/parser.py`に実装する。全モデルはPydantic `BaseModel`を継承する。

```python
class ColumnDef(BaseModel):
    name: str
    logical_name: str
    type: str               # パース時に自動で upper() する field_validator を実装
    not_null: bool
    description: str = ""   # 省略時は空文字列
    allowed_values: list[str] = []  # 省略時は空リスト（チェックなし）
    format: str | None = None       # DATE/TIMESTAMP/TIME型のみ有効。他型に指定した場合はValidationError

# model_validator で以下を検証:
#   - format が指定されている場合、type が DATE/TIMESTAMP/TIME のいずれかであること
#   （DATE系型の定義は DATETIME_TYPES セットとして loader.py と共有する）

class PrimaryKeyDef(BaseModel):
    columns: list[str]

class UniqueDef(BaseModel):
    columns: list[str]

class FKReference(BaseModel):
    table: str
    columns: list[str]

class ForeignKeyDef(BaseModel):
    columns: list[str]
    references: FKReference

class CheckDef(BaseModel):
    description: str
    query: str
    expect_zero: bool = True

class TableConstraints(BaseModel):
    # 全フィールド必須。制約なしの場合は空配列を渡すこと
    primary_key: list[PrimaryKeyDef]
    foreign_keys: list[ForeignKeyDef]
    unique: list[UniqueDef]
    checks: list[CheckDef]
    aggregation_checks: list[CheckDef]

class ExportDef(BaseModel):
    partition_by: list[str] = []    # 省略時は空リスト（パーティションなし）

class TableMeta(BaseModel):
    name: str
    description: str
    source_dir: str         # field_validator でディレクトリ存在確認

class TableDef(BaseModel):
    table: TableMeta
    columns: list[ColumnDef]
    table_constraints: TableConstraints  # 必須（デフォルトなし）
    export: ExportDef = ExportDef()      # 省略時はExportDef()（エクスポートなし）
    # model_validator(mode="after") で以下を検証:
    #   - columns が1件以上
    #   - primary_key / unique / foreign_keys に指定されたカラムが columns に存在する
    #   - export.partition_by に指定されたカラムが columns に存在する
    #   - FK参照先テーブルの存在確認は build_load_order() 側で行うため、ここでは行わない
```

### リレーションデータモデル

`tval/relation.py`に実装する。全モデルはPydantic `BaseModel`を継承する。

```python
class RelationEndpoint(BaseModel):
    """リレーションの片側（テーブル＋カラム）"""
    table: str
    columns: list[str]

class RelationDef(BaseModel):
    """リレーション定義"""
    name: str
    cardinality: Literal["1:1", "1:N", "N:1", "N:N"]
    from_: RelationEndpoint  # YAML上は "from"。読込時にリネーム
    to: RelationEndpoint

class RelationsConfig(BaseModel):
    """relations.yaml のトップレベルモデル"""
    relations: list[RelationDef]
```

### バリデーション責務の分離

| バリデーション内容 | 実施箇所 |
|---|---|
| YAMLスキーマ構造の正当性 | Pydantic（`parser.py`） |
| `table_constraints`の必須確認 | Pydantic（`parser.py`）|
| 自テーブル内のカラム参照整合性（PK/UNIQUE/FK自側） | Pydantic `model_validator`（`parser.py`） |
| `source_dir`の存在確認 | Pydantic `field_validator`（`parser.py`） |
| `export.partition_by`に指定されたカラムが`columns`に存在するか | Pydantic `model_validator`（`parser.py`） |
| `format`がDATE/TIMESTAMP/TIME以外の型に指定されていないか | Pydantic `model_validator`（`parser.py`） |
| `database_path`の拡張子確認（`.duckdb`必須） | `main.py`起動時 |
| FK参照先テーブルの存在確認 | `build_load_order()`（`builder.py`） |
| 循環依存の検出 | `build_load_order()`（`builder.py`） |
| データ型・NULL・PK・FK・UNIQUE制約 | DuckDB（INSERTエラーとして検出） |
| ロジックバリデーション | `checker.py`（read_only接続で実行） |
| リレーションのテーブル/カラム参照整合性 | `validate_relation_refs()`（`relation.py`） |
| リレーションカーディナリティ検証（一意性・参照整合性） | `run_relation_checks()`（`relation.py`、read_only接続で実行） |

---

## 8. モジュール仕様

### 8.1 `validator/parser.py`

**責務**: スキーマYAMLの読み込みとPydanticバリデーション

**公開関数**

```python
def load_table_definition(path: str | Path) -> TableDef:
    """単一YAMLファイルを読み込みTableDefを返す。バリデーション失敗時はpydantic.ValidationErrorを送出"""

def load_table_definitions(schema_dir: str | Path) -> list[TableDef]:
    """schema_dir以下の全*.yamlを読み込む。ファイルが0件の場合はFileNotFoundErrorを送出"""
```

---

### 8.2 `validator/builder.py`

**責務**: ロード順決定（DAG）・CREATE TABLE SQL生成・テーブル作成実行

**公開関数**

```python
def build_load_order(table_defs: list[TableDef]) -> list[TableDef]:
    """
    外部キー依存関係からDAGを構築し、トポロジカルソート順でTableDefリストを返す。

    使用ライブラリ: graphlib.TopologicalSorter（Python 3.9標準）
    循環依存検出時: graphlib.CycleError をキャッチしてValueErrorとして再送出
    FK参照先テーブルがschema_dir内に未定義の場合: ValueErrorを送出

    DAG構築ルール:
      graph = { テーブル名: {依存先テーブル名, ...} }
      依存先（参照される側）が先にロードされる
    """

def build_create_table_sql(tdef: TableDef) -> str:
    """
    TableDefからCREATE TABLE文字列を生成して返す。
    全識別子（テーブル名・カラム名）はquote_identifier()でクォート済みであること。

    生成する制約:
      - カラムレベル: NOT NULL
      - テーブルレベル: PRIMARY KEY, UNIQUE, FOREIGN KEY
      - CHECKは生成しない（ロード後にchecker.pyで検証するため）
    """

def create_tables(conn: duckdb.DuckDBPyConnection, table_defs: list[TableDef]) -> None:
    """build_load_order順でCREATE TABLEを実行する"""
```

**識別子のクォート処理**

`builder.py`内に以下を実装し、全モジュールから`from .builder import quote_identifier`でインポートして使用する。

```python
import re

def validate_identifier(name: str) -> str:
    """英字またはアンダースコア始まり、英数字・アンダースコアのみ許可。違反時はValueError"""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

def quote_identifier(name: str) -> str:
    """識別子をダブルクォートでエスケープする"""
    return f'"{validate_identifier(name)}"'
```

---

### 8.3 `validator/loader.py`

**責務**: ファイルをDuckDBテーブルにINSERT・エラーパース

**データクラス**

```python
@dataclass
class LoadError:
    file_path: str
    error_type: str   # "TYPE_MISMATCH" | "NOT_NULL" | "COLUMN_MISMATCH" | "FK_VIOLATION" | "UNIQUE_VIOLATION" | "ENCODING_DETECTION_FAILED" | "UNKNOWN"
    column: str | None
    row: int | None
    raw_message: str  # 必ず保持する。パース失敗時のフォールバック
```

**公開関数**

```python
def load_files(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef
) -> list[LoadError]:
    """
    tdef.table.source_dir 以下の対応拡張子ファイルを1件ずつINSERTし、
    発生したエラーをLoadErrorのリストで返す。

    対応拡張子: .csv, .xlsx, .parquet
    非対応拡張子: スキップ（警告ログ出力）
    .xls: LoadErrorとして記録（error_type="UNSUPPORTED_FORMAT"）

    ファイルが0件の場合: LoadError(error_type="NO_FILES")を返す
    """
```

**ファイル別INSERTクエリ**

```python
# CSV（文字コード検出・変換あり。詳細は後述）
# UTF-8/ASCIIの場合は元ファイルをそのまま渡し、非UTF-8の場合はUTF-8一時ファイルを作成する
conn.execute(
    f"INSERT INTO {quote_identifier(table_name)} SELECT * FROM read_csv(?, header=true, columns={columns_override})",
    [resolved_path]   # UTF-8/ASCII: 元ファイルパス、非UTF-8: 一時ファイルパス
)

# Parquet
conn.execute(
    f"INSERT INTO {quote_identifier(table_name)} SELECT * FROM read_parquet(?)",
    [file_path]
)

# XLSX（DuckDBネイティブ。拡張不要）
conn.execute(
    f"INSERT INTO {quote_identifier(table_name)} SELECT * FROM read_xlsx(?, header=true)",
    [file_path]
)
```

**`format`指定カラムがある場合のINSERT**

`ColumnDef.format`が指定されているカラムが1件以上存在する場合、`SELECT *`ではなくカラムを明示したSELECTを生成する。

`format`指定カラムは以下の手順で処理する。

1. `read_csv`/`read_parquet`/`read_xlsx`の`columns`パラメータで当該カラムの型を`VARCHAR`に上書きして読む
2. SELECTで`STRPTIME(col, format)::TYPE`にキャストする

```python
DATETIME_TYPES = {"DATE", "TIMESTAMP", "TIME"}

def _build_insert_select(tdef: TableDef) -> str:
    """
    formatが指定されたカラムがある場合は明示的なSELECTを生成する。
    formatが1件もない場合は"SELECT *"を返す。
    """
    format_cols = {col.name: col for col in tdef.columns if col.format}
    if not format_cols:
        return "SELECT *"

    parts = []
    for col in tdef.columns:
        qname = quote_identifier(col.name)
        if col.format:
            fmt = col.format.replace("'", "''")  # シングルクォートエスケープ
            parts.append(
                f"STRPTIME({qname}, '{fmt}')::{col.type} AS {qname}"
            )
        else:
            parts.append(qname)
    return "SELECT " + ", ".join(parts)


def _build_columns_override(tdef: TableDef) -> str:
    """
    read_csv/read_xlsxのcolumnsパラメータ文字列を生成する。
    常にテーブル定義の型を明示的に指定する。
    formatが指定されたカラムはVARCHARに上書きする。
    """
    format_cols = {col.name for col in tdef.columns if col.format}

    entries = []
    for col in tdef.columns:
        typ = "VARCHAR" if col.name in format_cols else col.type
        entries.append(f"'{col.name}': '{typ}'")
    return "{" + ", ".join(entries) + "}"
```

CSVの場合のINSERT例（`format`指定カラムあり）：

```sql
INSERT INTO "orders"
SELECT
    "order_id",
    STRPTIME("created_at", '%Y/%m/%d %H:%M:%S')::TIMESTAMP AS "created_at"
FROM read_csv(
    ?,
    header=true,
    columns={'order_id': 'INTEGER', 'created_at': 'VARCHAR'}
)
```

**CSVの文字コード検出・変換**

CSVファイルのロード前に以下の処理を行う。`chardet`で文字コードを検出し（先頭8KBのみサンプリング）、UTF-8/ASCIIの場合は元ファイルをそのまま`read_csv`に渡す。非UTF-8の場合はストリーミングでUTF-8一時ファイルに変換してから渡す。一時ファイルは`_insert_file()`の終了時（成功・失敗問わず）に削除する。

```python
import chardet
import shutil
import tempfile
from pathlib import Path

class EncodingDetectionError(Exception):
    """chardetの信頼度が閾値未満の場合に送出する"""
    pass

def _resolve_csv_path(
    file_path: str,
    confidence_threshold: float,
) -> tuple[str, bool]:
    """
    CSVファイルの文字コードを検出する。UTF-8/ASCIIの場合は元ファイルパスを
    そのまま返し、非UTF-8の場合はストリーミングでUTF-8一時ファイルに変換する。

    信頼度がconfidence_threshold未満の場合はEncodingDetectionErrorを送出する。

    Returns:
        UTF-8/ASCII: (元ファイルパス, False)
        非UTF-8: (一時ファイルのパス, True)

    Raises:
        EncodingDetectionError: 信頼度が閾値未満の場合
    """
    _CHARDET_SAMPLE_SIZE = 8192
    with open(file_path, "rb") as f:
        sample = f.read(_CHARDET_SAMPLE_SIZE)

    detected = chardet.detect(sample)
    encoding = detected.get("encoding") or "utf-8"
    confidence = detected.get("confidence") or 0.0

    # 信頼度が閾値未満の場合はエラー
    if confidence < confidence_threshold:
        raise EncodingDetectionError(
            f"文字コード検出の信頼度が閾値未満です "
            f"(detected={encoding}, confidence={confidence:.2f}, "
            f"threshold={confidence_threshold})"
        )

    # UTF-8/ASCIIはそのまま返す（I/O・メモリ削減）
    if encoding.lower().replace("-", "") in ("utf8", "ascii"):
        return file_path, False

    # 非UTF-8: ストリーミングでUTF-8一時ファイルに変換
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".csv",
        delete=False,
    )
    with open(file_path, "r", encoding=encoding, errors="replace") as src, tmp:
        shutil.copyfileobj(src, tmp)
    return tmp.name, True
```

`load_files()`は`confidence_threshold`を引数として受け取り、`_resolve_csv_path()`に渡す。

```python
def load_files(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    confidence_threshold: float = 0.8,
) -> list[LoadError]:
```

`load_files()`内でのCSV処理フロー：

```python
for file_path in csv_files:
    resolved_path, is_tmp = None, False
    try:
        resolved_path, is_tmp = _resolve_csv_path(file_path, confidence_threshold)
        conn.execute(
            f"INSERT INTO {quote_identifier(table_name)} "
            f"SELECT * FROM read_csv(?, header=true, columns={columns_override})",
            [resolved_path]
        )
    except EncodingDetectionError as e:
        errors.append(LoadError(
            file_path=file_path,
            error_type="ENCODING_DETECTION_FAILED",
            column=None,
            row=None,
            raw_message=str(e),
        ))
    except Exception as e:
        errors.append(parse_duckdb_error(file_path, str(e)))
    finally:
        if is_tmp and resolved_path:
            Path(resolved_path).unlink(missing_ok=True)
```

**ログ出力（文字コード変換時）**

非UTF-8の文字コード変換が発生した場合はINFOレベルでログを出力すること。UTF-8/ASCIIの場合はログ出力しない。

```python
logger.info(
    "Converting CSV to temporary UTF-8 file",
    extra={
        "file": file_path,
        "detected_encoding": encoding,
        "confidence": detected.get("confidence"),
    }
)
```

**DuckDBエラーパースパターン**

パターンマッチが成功した場合は構造化情報を、失敗した場合は`error_type="UNKNOWN"`として`raw_message`のみ保持する。

| error_type | 正規表現パターン（例） |
|---|---|
| `TYPE_MISMATCH` | `Could not convert .+ to (\w+) in column "(\w+)", Row: (\d+)` |
| `NOT_NULL` | `NOT NULL constraint failed: \w+\.(\w+)` |
| `COLUMN_MISMATCH` | `has (\d+) columns but (\d+) values` |
| `FK_VIOLATION` | `Violates foreign key constraint because key .+ does not exist` |
| `UNIQUE_VIOLATION` | `Duplicate key .+ violates (primary key\|unique) constraint` |
| `UNKNOWN` | 上記いずれにもマッチしない場合 |

**重要**: DuckDBのエラーメッセージはバージョンアップで変更される可能性がある。`raw_message`を必ず保持し、パース結果が不正確でも情報がゼロにならないようにすること。

---

### 8.4 `validator/checker.py`

**責務**: `checks`・`aggregation_checks`のSQL実行とOK/NG判定

**データクラス**

```python
@dataclass
class CheckResult:
    description: str
    query: str          # 実際に実行したSQL（{table}置換済み）
    status: str         # "OK" | "NG" | "SKIPPED"
    result_count: int | None  # クエリが返した件数
    message: str        # エラー詳細またはスキップ理由
```

**公開関数**

```python
def run_checks(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    load_errors: list[LoadError]
) -> tuple[list[CheckResult], list[CheckResult]]:
    """
    以下の順序でチェックを実行する。

    1. ColumnDef.allowed_valuesが1件以上のカラムに対して自動生成SQLで検証
    2. table_constraints.checks を実行
    3. table_constraints.aggregation_checks を実行

    ロードエラーが1件以上存在する場合は全チェックをSKIPPEDとして返す
    （不完全なデータに対してチェックを実行しても意味がないため）。

    {table}プレースホルダをtdef.table.nameに置換してSQLを実行する。
    expect_zero=trueの場合: 結果が0件 → OK、1件以上 → NG
    SQLエラー発生時: status="SKIPPED", messageにエラー内容を記録

    Returns: (checks_results, aggregation_checks_results)
    """
```

**`allowed_values`の自動SQL変換**

`ColumnDef.allowed_values`が空でないカラムに対して、以下のSQLを自動生成して実行する。
値は`?`プレースホルダでバインドできないため、文字列をシングルクォートでエスケープして埋め込む。
シングルクォートのエスケープは`value.replace("'", "''")`で行うこと。

```python
def _build_allowed_values_check(table_name: str, col: ColumnDef) -> CheckDef:
    escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in col.allowed_values)
    return CheckDef(
        description=f"{col.logical_name}（{col.name}）の許容値チェック",
        query=f"SELECT COUNT(*) FROM {{table}} WHERE {quote_identifier(col.name)} NOT IN ({escaped}) AND {quote_identifier(col.name)} IS NOT NULL",
        expect_zero=True,
    )
```

`allowed_values`チェックの結果は`checks_results`に含めて返す（`aggregation_checks_results`ではない）。

---

### 8.4.1 `tval/relation.py`

**責務**: テーブル間リレーションのカーディナリティ検証

**公開関数**

```python
def load_relations(path: str | Path) -> list[RelationDef]:
    """relations.yamlを読み込み、RelationDefリストを返す。
    YAML上の 'from' キーを 'from_' にリネームしてPydanticに渡す。"""

def validate_relation_refs(
    relations: list[RelationDef],
    table_defs: list[TableDef],
) -> None:
    """リレーションで参照されるテーブル・カラムがschema定義に存在することを検証。
    未定義の場合はValueErrorを送出。"""

def run_relation_checks(
    conn: duckdb.DuckDBPyConnection,
    relations: list[RelationDef],
    all_load_errors: dict[str, list[LoadError]],
) -> list[CheckResult]:
    """全リレーションのカーディナリティ検証を実行し、CheckResultリストを返す。
    ロードエラーがあるテーブルを含むリレーションは全チェックSKIPPED。"""
```

**内部関数**

```python
def _build_uniqueness_sql(table: str, cols: list[str]) -> str:
    """重複キー組み合わせの件数を返すSQL。0ならチェックOK。"""

def _build_referential_sql(
    source_table: str, source_cols: list[str],
    target_table: str, target_cols: list[str],
) -> str:
    """孤立行の件数を返すSQL。NULLは除外。0ならチェックOK。"""

def _build_relation_checks(rel: RelationDef) -> list[tuple[str, str]]:
    """カーディナリティに応じた(description, sql)ペアリストを生成。"""
```

**パイプライン統合**

- `main.py`にて、スキーマ読込後に`load_relations()` + `validate_relation_refs()`を実行
- read-only接続ブロック内で`run_relation_checks()`を実行
- エクスポート判定時はテーブル結果とリレーション結果の両方を考慮
- `generate_report()`に`relation_check_results`を渡す

---

### 8.5 `validator/profiler.py`

**責務**: 列ごとの基本統計量取得

**数値型の定義**

以下のDuckDB型を数値型として扱う。型名の判定はYAML定義の`type`フィールド（大文字化済み）で行う。

```python
NUMERIC_TYPES = {
    "INTEGER", "INT", "INT4", "INT32",
    "BIGINT", "INT8", "INT64",
    "SMALLINT", "INT2", "INT16",
    "TINYINT", "INT1",
    "HUGEINT",
    "FLOAT", "FLOAT4", "REAL",
    "DOUBLE", "FLOAT8",
    "DECIMAL", "NUMERIC",
}

# loader.pyと共有する定数。どちらかに定義してもう一方でインポートすること
DATETIME_TYPES = {"DATE", "TIMESTAMP", "TIME"}
```

**データクラス**

```python
@dataclass
class ColumnProfile:
    column_name: str
    logical_name: str       # TableDefのColumnDef.logical_nameから取得
    column_type: str        # YAML定義のtype（大文字化済み）
    is_numeric: bool

    # 全データ型共通（数値型カラムにも必ず算出する）
    count: int              # 総件数（NULL含む）
    not_null_count: int     # NOT NULL件数
    unique_count: int       # ユニーク件数（NULL除く）

    # 数値型限定（is_numeric=Falseの場合はNone）
    mean: float | None
    std: float | None
    skewness: float | None
    kurtosis: float | None
    min: float | None
    p25: float | None
    median: float | None
    p75: float | None
    max: float | None
```

**公開関数**

```python
def profile_table(
    conn: duckdb.DuckDBPyConnection,
    tdef: TableDef,
    load_errors: list[LoadError]
) -> list[ColumnProfile]:
    """
    テーブルの全列について基本統計量を取得しColumnProfileリストで返す。

    ロードエラーが1件以上存在する場合は空リストを返す。
    テーブルが空（0件）の場合は空リストを返す。
    """
```

**実装方針**

全データ型共通の統計量（`count`, `not_null_count`, `unique_count`）は以下のSQLで全列を一括取得する。

```sql
SELECT
    COUNT(*) AS count,
    COUNT({col}) AS not_null_count,
    COUNT(DISTINCT {col}) AS unique_count
FROM {table}
```

数値型列の追加統計量は、該当列に対して個別にSQLを実行する。

```sql
SELECT
    AVG({col})                       AS mean,
    STDDEV_SAMP({col})               AS std,
    SKEWNESS({col})                  AS skewness,
    KURTOSIS({col})                  AS kurtosis,
    MIN({col})                       AS min,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {col}) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS p75,
    MAX({col})                       AS max
FROM {table}
```

全識別子は`quote_identifier()`でクォートすること。

数値型の判定は`NUMERIC_TYPES`セットを使い、YAML定義の`type`フィールドで行う。DuckDBの`information_schema`は参照しない。

---

### 8.6 `validator/exporter.py`

**責務**: バリデーション済みテーブルをParquetファイルとして書き出す

**エクスポート先パス**

```
{config.yamlの親ディレクトリ}/tval/output/parquet/{table_name}/
```

パーティションなしの場合:
```
tval/output/parquet/orders/orders.parquet
```

パーティションありの場合（DuckDBのHive形式パーティション）:
```
tval/output/parquet/orders/created_at=2024-01/
tval/output/parquet/orders/created_at=2024-02/
    ...
```

**公開関数**

```python
def export_table(
    conn: duckdb.DuckDBPyConnection,   # read_only接続を渡すこと
    tdef: TableDef,
    output_base_dir: str | Path,
) -> ExportResult:
    """
    テーブルをParquetに書き出す。

    partition_byが空リストの場合:
        COPY {table} TO '{output_dir}/{table}.parquet' (FORMAT parquet)
    partition_byが指定されている場合:
        COPY {table} TO '{output_dir}' (FORMAT parquet, PARTITION_BY ({cols}), OVERWRITE_OR_IGNORE)

    出力先ディレクトリが存在しない場合は作成する。
    全識別子はquote_identifier()でクォートすること。
    パス文字列はSQLに直接埋め込むため、?プレースホルダが使えない点に注意。
    出力パスは Path.resolve() で正規化してからSQLに埋め込む。
    """
```

**データクラス**

```python
@dataclass
class ExportResult:
    table_name: str
    status: str           # "OK" | "SKIPPED" | "ERROR"
    output_path: str      # 実際に書き出したパス
    message: str = ""     # エラー内容またはスキップ理由
```

**エクスポートのSQL実装**

```python
# パーティションなし
conn.execute(f"""
    COPY {quote_identifier(table_name)}
    TO '{output_file}'
    (FORMAT parquet)
""")

# パーティションあり
partition_cols = ", ".join(quote_identifier(c) for c in partition_by)
conn.execute(f"""
    COPY {quote_identifier(table_name)}
    TO '{output_dir}'
    (FORMAT parquet, PARTITION_BY ({partition_cols}), OVERWRITE_OR_IGNORE)
""")
```

---

### 8.7 `validator/reporter.py`

**責務**: Jinja2テンプレートを使ったHTMLレポート生成

**データクラス**

```python
@dataclass
class TableReport:
    table_def: TableDef
    load_errors: list[LoadError]
    check_results: list[CheckResult]
    agg_check_results: list[CheckResult]
    profiles: list[ColumnProfile]       # 空リストの場合は統計量セクションを非表示
    export_result: ExportResult | None  # --exportフラグなし時はNone

    @property
    def overall_status(self) -> str:
        """
        NG判定ルール:
          - load_errorsが1件以上 → "NG"
          - check_results/agg_check_resultsにNGが1件以上 → "NG"
          - 全てOKまたはSKIPPED → "OK"
        """
```

**公開関数**

```python
def generate_report(
    table_reports: list[TableReport],
    output_path: str,
    db_path: str,
    executed_at: str,  # ISO 8601形式
) -> None:
    """Jinja2でHTMLを生成しoutput_pathに書き出す"""
```

---

### 8.8 `main.py`

```python
import yaml
import duckdb
from pathlib import Path
from datetime import datetime

from .parser import load_table_definitions
from .builder import build_load_order, create_tables
from .loader import load_files
from .checker import run_checks
from .profiler import profile_table
from .reporter import TableReport, generate_report

def run(config_path: str | None, export: bool = False) -> None:
    # 1. config探索
    if config_path is None:
        for candidate in ["./tval/config.yaml", "./config.yaml"]:
            if Path(candidate).exists():
                config_path = candidate
                break
        else:
            raise FileNotFoundError(
                "config.yaml が見つかりません。"
                "--config で明示指定するか、./tval/config.yaml を作成してください。"
            )

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 2. database_pathの拡張子検証
    db_path = Path(config["database_path"])
    if db_path.suffix != ".duckdb":
        raise ValueError(
            f"database_path の拡張子は .duckdb である必要があります: {db_path}"
        )

    # 3. スキーマYAML読み込み（project_rootを渡してパストラバーサル防止）
    project_root = Path(config_path).resolve().parent
    table_defs = load_table_definitions(config["schema_dir"], project_root=project_root)

    # 4. DAGによるロード順決定
    ordered_defs = build_load_order(table_defs)

    # 5. DuckDB接続（既存ファイルは削除して再作成）
    if db_path.exists():
        db_path.unlink()
    conn_rw = duckdb.connect(str(db_path))

    # 6. テーブル作成・ファイルロード
    create_tables(conn_rw, ordered_defs)
    confidence_threshold = config.get("encoding_confidence_threshold", 0.8)

    table_reports = []
    for tdef in ordered_defs:
        load_errors = load_files(conn_rw, tdef, confidence_threshold=confidence_threshold)

        # 7. checks/profilerはread_only接続で実行
        conn_ro = duckdb.connect(str(db_path), read_only=True)
        check_results, agg_check_results = run_checks(conn_ro, tdef, load_errors)
        profiles = profile_table(conn_ro, tdef, load_errors)
        conn_ro.close()

        table_reports.append(TableReport(
            table_def=tdef,
            load_errors=load_errors,
            check_results=check_results,
            agg_check_results=agg_check_results,
            profiles=profiles,
            export_result=None,
        ))

    conn_rw.close()

    # 8. エクスポート（--exportフラグがある場合のみ）
    # 1テーブルでもNGがあれば全テーブルのエクスポートをスキップ
    if export:
        all_ok = all(r.overall_status == "OK" for r in table_reports)
        output_base_dir = project_root / "tval" / "output" / "parquet"
        conn_ro = duckdb.connect(str(db_path), read_only=True)
        for i, (report, tdef) in enumerate(zip(table_reports, ordered_defs)):
            if not all_ok:
                report.export_result = ExportResult(
                    table_name=tdef.table.name,
                    status="SKIPPED",
                    output_path="",
                    message="バリデーションNGのテーブルが存在するためスキップしました",
                )
            else:
                report.export_result = export_table(conn_ro, tdef, output_base_dir)
        conn_ro.close()

    # 8. レポート生成
    output_path = config["output_path"]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    generate_report(
        table_reports=table_reports,
        output_path=output_path,
        db_path=str(db_path),
        executed_at=datetime.now().isoformat(),
    )
```

---

## 9. セキュリティ仕様

### 識別子インジェクション対策（2層）

SQLのテーブル名・カラム名はパラメータバインドが使えないため以下で対処する。

**第1層: ホワイトリスト検証**

```python
re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name)
```

英字またはアンダースコア始まり、英数字・アンダースコアのみ許可。違反時は`ValueError`。

**第2層: ダブルクォートエスケープ**

```python
f'"{validated_name}"'
```

**値のバインド**

全ての値は`?`プレースホルダでバインドする。

```python
conn.execute("SELECT * FROM read_csv(?, header=true, columns=...)", [file_path])
```

### `checks`/`aggregation_checks`クエリについて

内部ツールのため、ユーザーが任意のSQLを記述できる設計を許容する。
ただし**checksクエリ実行時はread_only接続を別途張る**ことでデータ破壊・ファイル書き出し・ネットワークアクセスを防止する。

```python
# main.py内での接続分離
conn_rw = duckdb.connect(str(db_path))   # ロード・テーブル作成用（read/write）
conn_ro = duckdb.connect(str(db_path), read_only=True)  # checks実行用（read only）
```

`checker.py`と`profiler.py`には`conn_ro`を渡す。`builder.py`と`loader.py`には`conn_rw`を渡す。

### `database_path`の誤削除防止

`tval run`起動時に`database_path`の拡張子を検証する。`.duckdb`以外の拡張子が指定された場合はエラーを表示して即時終了する。

```python
db_path = Path(config["database_path"])
if db_path.suffix != ".duckdb":
    raise ValueError(
        f"database_path の拡張子は .duckdb である必要があります: {db_path}"
    )
```

### `source_dir`のパストラバーサル防止

`parser.py`の`source_dir`バリデーションにパス正規化を追加する。
`config.yaml`のパスを基準とした**プロジェクトルート**（`config.yaml`の親ディレクトリ）以下に収まっているかを確認する。

```python
@model_validator(mode="after")
def source_dir_within_project(self) -> "TableMeta":
    # project_rootはTableMetaの外部から注入する（後述）
    # resolved_source_dirがproject_root以下でなければエラー
    resolved = Path(self.source_dir).resolve()
    if not str(resolved).startswith(str(self._project_root.resolve())):
        raise ValueError(
            f"source_dir はプロジェクトルート以下である必要があります: {self.source_dir}"
        )
    return self
```

`project_root`は`load_table_definitions(schema_dir, project_root)`の引数として渡す。
`main.py`では`config.yaml`の親ディレクトリを`project_root`として渡す。

```python
project_root = Path("config.yaml").resolve().parent
table_defs = load_table_definitions(config["schema_dir"], project_root=project_root)
```

---

## 10. エラーハンドリング仕様

### 起動時エラー（即時終了）

| 条件 | 例外 | 対処 |
|---|---|---|
| スキーマYAMLのフォーマット違反 | `pydantic.ValidationError` | スタックトレースをそのまま表示 |
| `source_dir`が存在しない | `pydantic.ValidationError` | 同上 |
| FK参照先テーブルがschema_dir内に未定義 | `ValueError` | メッセージを表示して終了 |
| 循環依存 | `ValueError`（`CycleError`をラップ） | 関係するテーブル名を表示して終了 |
| `schema_dir`にYAMLが0件 | `FileNotFoundError` | メッセージを表示して終了 |
| リレーション定義が未定義テーブル/カラムを参照 | `ValueError` | メッセージを表示して終了 |

### 実行時エラー（レポートに記録して継続）

| 条件 | 記録先 | 挙動 |
|---|---|---|
| INSERTエラー | `LoadError` | 当該ファイルをNGとしてレポートに記録。次のファイルに進む |
| checksクエリエラー | `CheckResult(status="ERROR")` | エラー内容をmessageに記録 |
| プロファイリングエラー | `ColumnProfile(error=...)` | エラー内容をerrorフィールドに記録 |
| リレーションチェックSQL実行エラー | `CheckResult(status="ERROR")` | エラー内容をmessageに記録 |
| ロードエラーのあるテーブルを含むリレーション | `CheckResult(status="SKIPPED")` | スキップ理由をmessageに記録 |

---

## 11. HTMLレポート仕様

### 全体構成

```
[サマリーセクション]
  - 実行日時
  - 対象DBパス
  - 総テーブル数 / OK件数 / NG件数
  - エクスポート実行有無（--exportフラグがあった場合のみ表示）

[テーブル別セクション（テーブル数分繰り返し）]
  - テーブル名・論理名・ステータス（OK/NG）
  - ファイルロード結果
  - ロジックバリデーション結果（checks）
  - 集計バリデーション結果（aggregation_checks）
  - 基本統計量テーブル
  - エクスポート結果（export_resultがNoneでない場合のみ表示）

[リレーションカーディナリティ検証セクション（relations_path設定時のみ表示）]
  - リレーション検証サマリー（総数 / OK / NG / SKIPPED）
  - 各リレーションチェック結果（ステータス・説明・クエリ・メッセージ）
```

### ステータス表示

| ステータス | 表示 |
|---|---|
| OK | ✅ |
| NG | ❌ |
| ERROR | ❌ |
| SKIPPED | ⚠️ |

### Jinja2テンプレート変数

`reporter.py`がJinja2に渡すコンテキスト:

```python
{
    "executed_at": str,           # ISO 8601
    "db_path": str,
    "table_reports": list[TableReport],
    "summary": {
        "total": int,
        "ok": int,
        "ng": int,
    },
    "relation_check_results": list[CheckResult],  # リレーション検証結果
    "relation_summary": {
        "total": int,
        "ok": int,
        "ng": int,
        "skipped": int,
    },
}
```

---

## 12. 実装順序

依存関係に従い以下の順で実装・動作確認すること。

1. **`logger.py`**: `get_logger()`でJSONが出力されることを確認
2. **`parser.py`** + **`tests/test_parser.py`**: YAMLパースと全バリデーション異常系のテストが通ることを確認
3. **`builder.py`** + **`tests/test_builder.py`**: DAGソートと異常系のテストが通ることを確認
4. **`loader.py`** + **`tests/test_loader.py`**: エラーパースのテストが通ることを確認
5. **`checker.py`**: checksが実行されOK/NGが正しく判定されることを確認
6. **`profiler.py`**: 共通統計量の一括取得SQLと、数値型列への個別SQL実行が正しく動くことを確認
7. **`exporter.py`**: パーティションなし・ありの両パターンでParquet書き出しを確認
8. **`reporter.py` + テンプレート**: HTML出力を実装
9. **`main.py`** + **`tests/test_integration.py`**: エンドツーエンドテストが通ることを確認
10. **`init.py`**: スケルトン生成・`.gitignore`追記を実装
11. **`cli.py`**: `argparse`でサブコマンドを組み上げ、`tval init`・`tval run`・`tval run --export`が動くことを確認
12. **`pyproject.toml`**: `pip install -e .[dev]`でインストールし、`tval`コマンドと`pytest`が動くことを確認

---

## 13. 制約・既知の限界

| 制約 | 内容 |
|---|---|
| `.xls`非対応 | DuckDBの`read_xlsx`は`.xlsx`のみ対応。`.xls`は運用でブロックすること |
| DuckDBエラーパース | エラーメッセージ文字列はDuckDBのバージョンアップで変更される可能性がある。`raw_message`を保持することでフォールバックを確保している |
| 高カーディナリティ列のunique_count | `COUNT(DISTINCT col)`は列ごとに実行するため、カラム数×テーブルサイズに比例して実行時間が増加する |
| 数値型統計量の実行時間 | 数値型列ごとに個別SQLを実行するため、数値型列が多いテーブルでは実行時間が伸びる |
| checksクエリのSQL任意実行 | 内部ツールとして許容。外部公開する場合は再検討が必要 |
| テーブル定義書のフォーマット変更 | YAMLスキーマを変更した場合、Pydanticモデルも合わせて修正が必要 |
| マルチスキーマ非対応 | DuckDBの`main`スキーマのみを対象とする |
| リレーション検証の複合キー | 複合キーのリレーション検証は全カラムの一致を要求する。部分一致は未対応 |
| リレーション検証のNULL処理 | NULL値は参照整合性チェックから除外される（SQL FK標準に準拠）。NULLの存在そのものの検証が必要な場合はスキーマYAMLの`not_null`制約を使用すること |
| ER図生成 | 初期スコープ外。FK定義とrelations.yamlからMermaid.jsで生成可能だが、後続開発とする |
