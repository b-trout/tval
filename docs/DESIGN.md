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
14. [エラーハンドリング規約](#14-エラーハンドリング規約)

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
├── src/
│   └── tval/                # Pythonパッケージ本体
│       ├── __init__.py
│       ├── cli.py           # CLIエントリポイント（argparse）
│       ├── init.py          # tval initコマンドのロジック
│       ├── main.py          # tval runコマンドのロジック
│       ├── parser.py        # YAMLパース・Pydanticモデル定義・ProjectConfig
│       ├── builder.py       # CREATE TABLE生成・DAGロード順決定
│       ├── loader.py        # ファイルINSERT・DuckDBエラーパース
│       ├── checker.py       # ロジックバリデーション実行
│       ├── relation.py      # テーブル間リレーションカーディナリティ検証
│       ├── profiler.py      # 基本統計量算出
│       ├── exporter.py      # Parquetエクスポート
│       ├── reporter.py      # HTMLレポート生成
│       ├── logger.py        # 構造化JSONロガー
│       ├── status.py        # ステータスEnum定義（CheckStatus / ExportStatus）
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
│   ├── data/                # 受領データ置き場（.gitignore対象）
│   └── output/              # レポート出力先（.gitignore対象）
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

- Pythonバージョン: **3.10以上**必須
- XLSX読み込みはDuckDBの`read_xlsx`をネイティブ使用。`openpyxl`は不要
- `.xls`（旧Excel形式）は非対応。受領時に`.xlsx`への変換を運用ルールとする
- ロギングはPython標準`logging`モジュール＋カスタムJSONフォーマッターで実装

### 4.1 ロギング仕様

1行1JSONで標準エラー出力（stderr）に出力する。`logger.py`の`get_logger(__name__)`経由で使用する。

```json
{"timestamp": "2024-01-01T12:00:00.000000", "level": "INFO", "module": "loader", "message": "ファイルロード開始", "table": "orders", "file": "./data/orders/jan.csv"}
```

予約フィールドは `_RESERVED` セットとしてモジュールレベルに定義する。`record.__dict__`から予約外のフィールドをextraとして展開する。

**各モジュールのログ出力要件**

| モジュール | レベル | タイミング | 必須extraフィールド |
|---|---|---|---|
| `main.py` | INFO | ツール起動時・終了時 | - |
| `builder.py` | INFO | テーブル作成時 | `table` |
| `loader.py` | INFO | ファイルロード開始・完了時 | `table`, `file` |
| `loader.py` | INFO | CSV文字コードをUTF-8に変換した場合 | `file`, `detected_encoding`, `confidence` |
| `loader.py` | ERROR | CSV文字コード検出の信頼度が閾値未満 | `file`, `detected_encoding`, `confidence`, `threshold` |
| `loader.py` | ERROR | ファイルロードエラー時 | `table`, `file`, `error_type` |
| `loader.py` | WARNING | 非対応拡張子スキップ時 | `file` |
| `checker.py` | INFO | チェック実行開始・完了時 | `table` |
| `checker.py` | WARNING | チェックSKIPPED時 | `table`, `check_description` |
| `checker.py` | ERROR | チェックNG時 | `table`, `check_description` |
| `profiler.py` | INFO | 統計量取得開始・完了時 | `table` |
| `profiler.py` | ERROR | 統計量取得失敗時 | `table`, `column` |

### 4.2 パッケージング仕様

| 項目 | 値 |
|---|---|
| PyPIパッケージ名 | `tval-cli` |
| CLIコマンド名 | `tval` |
| Pythonパッケージ名 | `tval` |

`src/`レイアウトを採用する。ビルドシステムは`hatchling` + `hatch-vcs`。

**静的解析設定**: ruff（`E`, `F`, `I`, `B`, `C90`）、mypy strict、mccabe max-complexity=15。

### 4.3 CLI仕様

`argparse`を使用する。外部ライブラリ（`click`等）は使用しない。

**`tval init [--dir PATH]`**

| オプション | デフォルト | 説明 |
|---|---|---|
| `--dir` | `./tval` | スケルトンを生成するディレクトリ |

生成物: `config.yaml`、`schema/`、`data/`、`output/`（各サブディレクトリに`.gitkeep`配置）。`.gitignore`に`tval/data/`と`tval/output/`を自動追記する。既存ディレクトリへの上書きは行わない。

**`tval run [--config PATH] [--export]`**

| オプション | 説明 |
|---|---|
| `--config` | `config.yaml`のパスを明示指定する |
| `--export` | バリデーション全成功時にParquetファイルを書き出す |

`--config`省略時は `./tval/config.yaml` → `./config.yaml` の順に探索する。

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
| `encoding_confidence_threshold` | float | ❌ | chardetの信頼度閾値（0.0〜1.0）。省略時は`0.8` |
| `relations_path` | string | ❌ | テーブル間リレーション定義ファイルのパス。省略時はリレーション検証をスキップ |

`config.yaml`は`ProjectConfig`（Pydanticモデル）でバリデーションされる。`database_path`の`.duckdb`拡張子検証はこのモデルの`field_validator`が行う。

---

## 6. スキーマYAML仕様

1テーブルにつき1ファイル。`schema_dir`以下に配置する。

### 完全な例

```yaml
table:
  name: orders
  description: 注文テーブル
  source_dir: ./data/orders/

columns:
  - name: order_id
    logical_name: 注文ID
    type: INTEGER
    not_null: true
    description: システム採番の注文識別子

  - name: amount
    logical_name: 金額
    type: DOUBLE
    not_null: true
    min: 0
    max: 1000000

  - name: status
    logical_name: ステータス
    type: VARCHAR
    not_null: true
    allowed_values: ["pending", "shipped", "cancelled"]

  - name: created_at
    logical_name: 作成日時
    type: TIMESTAMP
    not_null: true
    format: "%Y/%m/%d %H:%M:%S"

table_constraints:
  primary_key:
    columns: [order_id]
  unique:
    - columns: [order_id]
  foreign_keys:
    - columns: [user_id]
      references:
        table: users
        columns: [user_id]
  checks:
    - description: 合計は小計+税と一致すること
      query: "SELECT COUNT(*) FROM {table} WHERE total != sub_total + tax"
      expect_zero: true
  row_conditions:
    - description: 金額は注文IDの10倍以下であること
      condition: "amount <= order_id * 10"
  aggregation_checks:
    - description: pendingステータスの比率は50%未満であること
      query: |
        SELECT COUNT(*) FROM (
          SELECT 1 FROM {table}
          HAVING SUM(CASE WHEN status = 'pending' THEN 1.0 ELSE 0 END) / COUNT(*) >= 0.5
        )
      expect_zero: true

export:
  partition_by: [created_at]
```

### フィールド仕様

#### `table`（必須）

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | テーブル物理名（英数字・アンダースコアのみ） |
| `description` | string | ✅ | テーブルの概要説明 |
| `source_dir` | string | ✅ | 受領データディレクトリ。YAML読み込み時点で存在必須 |

#### `columns`（必須・1件以上）

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | カラム物理名 |
| `logical_name` | string | ✅ | カラム論理名（レポート表示用） |
| `type` | string | ✅ | DuckDB型名（自動大文字化）。`VARCHAR(N)`・`DECIMAL(p,s)`等の精度指定可 |
| `not_null` | boolean | ✅ | `true`でNOT NULL制約を付与 |
| `description` | string | ❌ | 補足説明。省略時は空文字列 |
| `allowed_values` | string[] | ❌ | 許容値リスト。`checker.py`が自動でSQLに変換して検証 |
| `format` | string | ❌ | DATE/TIMESTAMP/TIME型のフォーマット文字列。他型に指定時はValidationError |
| `min` | float | ❌ | 数値型カラムの最小値。`checker.py`が自動でレンジチェックSQLを生成。非数値型に指定時はValidationError |
| `max` | float | ❌ | 数値型カラムの最大値。`checker.py`が自動でレンジチェックSQLを生成。非数値型に指定時はValidationError |

#### `table_constraints`（**必須**）

制約がない場合も省略不可。全フィールドを空配列で明示すること。

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `primary_key` | array | ✅ | `columns`: string[]。制約なしの場合は`[]` |
| `unique` | array | ✅ | `columns`: string[]の配列。制約なしの場合は`[]` |
| `foreign_keys` | array | ✅ | `columns` + `references.table` + `references.columns`。制約なしの場合は`[]` |
| `checks` | array | ✅ | `description` + `query`（`{table}`プレースホルダ） + `expect_zero`（デフォルトtrue） |
| `aggregation_checks` | array | ✅ | checksと同一構造。レポート上で別セクションに表示される |
| `row_conditions` | array | ❌ | `description` + `condition`（SQLブール式）。`checker.py`が`SELECT COUNT(*) FROM {table} WHERE NOT (condition)`を自動生成。省略時は空配列 |

#### `export`（任意）

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `partition_by` | string[] | ❌ | パーティション列リスト。`columns`に存在する必要がある |

---

## 6.1 リレーションYAML仕様（`relations.yaml`）

テーブル間のカーディナリティ（1:1, 1:N, N:1, N:N）を定義するオプショナルなファイル。

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
```

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `name` | string | ✅ | リレーション名（レポート表示用） |
| `cardinality` | string | ✅ | `"1:1"`, `"1:N"`, `"N:1"`, `"N:N"` のいずれか |
| `from.table` / `from.columns` | string / string[] | ✅ | from側テーブル・カラム |
| `to.table` / `to.columns` | string / string[] | ✅ | to側テーブル・カラム |

### カーディナリティ別の検証内容

| カーディナリティ | 検証内容 | チェック数 |
|---|---|---|
| `1:1` | from側一意性 + to側一意性 + 双方向参照整合性 | 4 |
| `1:N` | from側（1側）一意性 + to→from参照整合性 | 2 |
| `N:1` | to側（1側）一意性 + from→to参照整合性 | 2 |
| `N:N` | 双方向参照整合性 | 2 |

**注意事項**: `from`はPython予約語のため内部では`from_`にリネーム。NULL値は参照整合性チェックから除外（SQL FK標準に準拠）。いずれかのテーブルにロードエラーがある場合は全チェック`SKIPPED`。

---

## 7. データモデル仕様

全モデルはPydantic `BaseModel`を継承する。詳細は`parser.py`および`relation.py`のソースコードを参照。

### 設定ファイルモデル（`parser.py`）

| モデル | フィールド | 説明 |
|---|---|---|
| `ProjectConfig` | `database_path`, `schema_dir`, `output_path`, `encoding_confidence_threshold`(=0.8), `relations_path`(任意) | config.yamlのバリデーション済みモデル。`database_path`の`.duckdb`拡張子を`field_validator`で検証 |

### スキーマ定義モデル（`parser.py`）

| モデル | 主要フィールド | 説明 |
|---|---|---|
| `ColumnDef` | `name`, `logical_name`, `type`, `not_null`, `description`, `allowed_values`, `format`, `min`, `max` | カラム定義。`type`は自動大文字化。`min`/`max`は数値型のみ |
| `TableMeta` | `name`, `description`, `source_dir` | テーブルメタ情報。`source_dir`の存在確認・パストラバーサル防止あり |
| `TableConstraints` | `primary_key`, `unique`, `foreign_keys`, `checks`, `aggregation_checks`, `row_conditions` | 必須フィールド（空配列可）。`row_conditions`はデフォルト空 |
| `RowConditionDef` | `description`, `condition` | 行レベル条件式。`condition`はSQLブール式 |
| `CheckDef` | `description`, `query`, `expect_zero`(=True) | チェック定義。`params`フィールドも内部で使用 |
| `TableDef` | `table`, `columns`, `table_constraints`, `export` | テーブル全体定義。`model_validator`でカラム参照整合性を検証 |
| `ExportDef` | `partition_by`(=[]) | エクスポート設定 |

### リレーションモデル（`relation.py`）

| モデル | 主要フィールド | 説明 |
|---|---|---|
| `RelationEndpoint` | `table`, `columns` | リレーションの片側 |
| `RelationDef` | `name`, `cardinality`, `from_`, `to` | リレーション定義 |
| `RelationsConfig` | `relations` | relations.yamlトップレベル |

### ステータスEnum（`status.py`）

| Enum | 値 | 説明 |
|---|---|---|
| `CheckStatus` | `OK`, `NG`, `SKIPPED`, `ERROR` | バリデーションチェック結果。`str`ミックスインで`== "OK"`互換 |
| `ExportStatus` | `OK`, `SKIPPED`, `ERROR` | エクスポート結果 |

### バリデーション責務の分離

| バリデーション内容 | 実施箇所 |
|---|---|
| YAMLスキーマ構造の正当性 | Pydantic（`parser.py`） |
| `table_constraints`の必須確認 | Pydantic（`parser.py`） |
| 自テーブル内のカラム参照整合性（PK/UNIQUE/FK自側） | Pydantic `model_validator`（`parser.py`） |
| `source_dir`の存在確認・パストラバーサル防止 | Pydantic `field_validator` / `model_validator`（`parser.py`） |
| `export.partition_by`のカラム存在確認 | Pydantic `model_validator`（`parser.py`） |
| `format`がDATE/TIMESTAMP/TIME以外に指定されていないか | Pydantic `model_validator`（`parser.py`） |
| `database_path`の拡張子確認（`.duckdb`必須） | `ProjectConfig.validate_db_extension()`（`parser.py`） |
| FK参照先テーブルの存在確認 | `build_load_order()`（`builder.py`） |
| 循環依存の検出 | `build_load_order()`（`builder.py`） |
| データ型・NULL・PK・FK・UNIQUE制約 | DuckDB（INSERTエラーとして検出） |
| ロジックバリデーション | `checker.py`（read_only接続で実行） |
| リレーション参照整合性 | `validate_relation_refs()` / `run_relation_checks()`（`relation.py`） |

---

## 8. モジュール仕様

### 8.1 `parser.py`

**責務**: スキーマYAMLの読み込み・Pydanticバリデーション・`ProjectConfig`定義

**公開関数**:
- `load_table_definition(path, project_root)` → `TableDef`
- `load_table_definitions(schema_dir, project_root)` → `list[TableDef]`（0件時は`FileNotFoundError`）

**定数**:
- `DATETIME_TYPES = {"DATE", "TIMESTAMP", "TIME"}`（`loader.py`と共有）
- `NUMERIC_TYPES`（INTEGER/BIGINT/SMALLINT等の数値型セット。`profiler.py`・`checker.py`と共有）

---

### 8.2 `builder.py`

**責務**: ロード順決定（DAG）・CREATE TABLE SQL生成・テーブル作成実行・識別子クォート

**公開関数**:
- `build_load_order(table_defs)` → `list[TableDef]`（トポロジカルソート順。`graphlib.TopologicalSorter`使用。循環依存時は`ValueError`）
- `build_create_table_sql(tdef)` → `str`（NOT NULL / PK / UNIQUE / FK制約付き。CHECKは生成しない）
- `create_tables(conn, table_defs)` → `None`
- `validate_identifier(name)` → `str`（`[A-Za-z_][A-Za-z0-9_]*`以外は`ValueError`）
- `quote_identifier(name)` → `str`（検証済み識別子をダブルクォート）

全モジュールから`from .builder import quote_identifier`でインポートして使用する。

---

### 8.3 `loader.py`

**責務**: ファイルをDuckDBテーブルにINSERT・エラーパース・文字コード検出

**データクラス**: `LoadError`（`file_path`, `error_type`, `column`, `row`, `raw_message`）

**公開関数**:
- `load_files(conn, tdef, confidence_threshold=0.8)` → `list[LoadError]`
- `parse_duckdb_error(file_path, message)` → `LoadError`

**対応拡張子**: `.csv`, `.xlsx`, `.parquet`。`.xls`は`UNSUPPORTED_FORMAT`。ファイル0件時は`NO_FILES`。

**CSV文字コード処理**: `chardet`で先頭8KBをサンプリング。UTF-8/ASCIIは元ファイルをそのまま渡す。非UTF-8はストリーミングでUTF-8一時ファイルに変換し、処理後に削除。信頼度が閾値未満の場合は`EncodingDetectionError`を送出し`ENCODING_DETECTION_FAILED`として記録。

**`format`指定カラムの処理**: `format`が指定されたカラムは`VARCHAR`として読み込み後、`STRPTIME(col, format)::TYPE`でキャスト。`_build_insert_select()`で明示的なSELECTを生成する（`SELECT *`は使用しない）。

**DuckDBエラーパースパターン**:

| error_type | マッチ対象 |
|---|---|
| `TYPE_MISMATCH` | `Could not convert .+ to ... in column ...` |
| `NOT_NULL` | `NOT NULL constraint failed: ...` |
| `COLUMN_MISMATCH` | `has N columns but M values` |
| `FK_VIOLATION` | `Violates foreign key constraint ...` |
| `UNIQUE_VIOLATION` | `Duplicate key ... violates ... constraint` |
| `UNKNOWN` | 上記いずれにもマッチしない場合 |

DuckDBのエラーメッセージはバージョンアップで変更される可能性がある。`raw_message`を必ず保持すること。

---

### 8.4 `checker.py`

**責務**: `checks`・`aggregation_checks`・`allowed_values`チェックのSQL実行とOK/NG判定

**データクラス**: `CheckResult`（`description`, `query`, `status: CheckStatus`, `result_count`, `message`）

**公開関数**:
- `make_skipped_result(check, table_name, message)` → `CheckResult`（`relation.py`からも使用）
- `run_checks(conn, tdef, load_errors)` → `tuple[list[CheckResult], list[CheckResult]]`

**定数**: `NUMERIC_TYPES`は`parser.py`で定義（`profiler.py`と共有）

**動作**:
1. `allowed_values`が指定されたカラムについて自動でSQLを生成・実行（`?`プレースホルダでバインド）
2. `min`/`max`が指定されたカラムについてレンジチェックSQLを自動生成・実行（NULL除外、`?`プレースホルダでバインド）
3. `table_constraints.row_conditions`について`SELECT COUNT(*) FROM {table} WHERE NOT (condition)`を自動生成・実行
4. `table_constraints.checks`を実行
5. `table_constraints.aggregation_checks`を実行
6. ロードエラーが1件以上ある場合は全チェックを`SKIPPED`
7. `expect_zero=true`の場合: 結果が0件 → OK、1件以上 → NG
8. SQLエラー発生時: `status=CheckStatus.ERROR`

---

### 8.4.1 `relation.py`

**責務**: テーブル間リレーションのカーディナリティ検証

**公開関数**:
- `load_relations(path)` → `list[RelationDef]`（`from`→`from_`リネーム処理含む）
- `validate_relation_refs(relations, table_defs)` → `None`（未定義テーブル/カラムで`ValueError`）
- `run_relation_checks(conn, relations, all_load_errors)` → `list[CheckResult]`

ロードエラーがあるテーブルを含むリレーションは全チェック`SKIPPED`。一意性チェックは`GROUP BY ... HAVING COUNT(*) > 1`、参照整合性チェックは`LEFT JOIN ... WHERE ... IS NULL`で実装。

---

### 8.5 `profiler.py`

**責務**: 列ごとの基本統計量取得

**データクラス**: `ColumnProfile`（`column_name`, `logical_name`, `column_type`, `is_numeric`, `count`, `not_null_count`, `unique_count`, + 数値型限定統計量, `error`）

**公開関数**:
- `profile_table(conn, tdef, load_errors)` → `list[ColumnProfile]`（ロードエラー時・空テーブル時は空リスト）

**数値型定義**: `NUMERIC_TYPES`セット（INTEGER/BIGINT/SMALLINT/TINYINT/HUGEINT/FLOAT/DOUBLE/DECIMAL等）。YAML定義の`type`フィールドで判定。

**統計量**: 全型共通（`COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`を一括SQL）。数値型追加（`AVG`, `STDDEV_SAMP`, `SKEWNESS`, `KURTOSIS`, `MIN`, `PERCENTILE_CONT(0.25/0.50/0.75)`, `MAX`を列ごと個別SQL）。日付・日時型（`DATETIME_TYPES`: DATE/TIMESTAMP/TIME）は`MIN`/`MAX`のみ計算し、値は`str()`で文字列化して格納。`ColumnProfile.is_temporal`フラグで判定。

---

### 8.6 `exporter.py`

**責務**: バリデーション済みテーブルをParquetファイルとして書き出す

**データクラス**: `ExportResult`（`table_name`, `status: ExportStatus`, `output_path`, `message`）

**公開関数**:
- `export_table(conn, tdef, output_base_dir)` → `ExportResult`

**出力先**: `{output_path_parent}/parquet/{table_name}/`。パーティションなしは`{table_name}.parquet`単一ファイル、パーティションありはDuckDBのHive形式ディレクトリ構造。

---

### 8.7 `reporter.py`

**責務**: Jinja2テンプレートを使ったHTMLレポート生成

**データクラス**: `TableReport`（`table_def`, `load_errors`, `check_results`, `agg_check_results`, `profiles`, `export_result`）。`overall_status`プロパティで`CheckStatus.NG`/`CheckStatus.OK`を返す。

**公開関数**:
- `generate_report(table_reports, output_path, db_path, executed_at, relation_check_results=[])` → `None`

---

### 8.8 `main.py`

**責務**: パイプライン全体のオーケストレーション

**公開関数**: `run(config_path=None, export=False)` → `None`

**ヘルパー関数**:
- `_discover_config_path(config_path)` → `str`（自動探索: `./tval/config.yaml` → `./config.yaml`）
- `_load_data(conn, ordered_defs, confidence_threshold)` → `dict[str, list[LoadError]]`
- `_build_table_reports(conn, ordered_defs, all_load_errors)` → `list[TableReport]`

**実行フロー**:
1. config探索 → `ProjectConfig`でバリデーション
2. パス解決（config.yaml親ディレクトリ基準）
3. スキーマYAML読み込み → リレーション読み込み（任意）
4. DAGロード順決定
5. DuckDB接続（既存ファイル削除・再作成）→ テーブル作成・データロード（read/write接続）
6. チェック・プロファイリング・リレーション検証（read-only接続）
7. エクスポート（`--export`時、全テーブル+リレーションOKの場合のみ。NGがあれば全テーブル`SKIPPED`）
8. HTMLレポート生成

---

## 9. セキュリティ仕様

### 識別子インジェクション対策（2層）

1. **ホワイトリスト検証**: `[A-Za-z_][A-Za-z0-9_]*`以外は`ValueError`
2. **ダブルクォートエスケープ**: 検証済み識別子を`"name"`形式でクォート

全ての値は`?`プレースホルダでバインドする。

### `checks`/`aggregation_checks`クエリ

内部ツールのため任意SQL実行を許容する。ただしchecksクエリ実行時はread_only接続を使用することでデータ破壊・ファイル書き出しを防止する。

### `database_path`の誤削除防止

`ProjectConfig`の`field_validator`で`.duckdb`拡張子を検証。不正な拡張子は`ValidationError`で即時終了。

### `source_dir`のパストラバーサル防止

`parser.py`の`model_validator`で`source_dir`をプロジェクトルート（`config.yaml`の親ディレクトリ）以下に制限。

---

## 10. エラーハンドリング仕様

### 起動時エラー（即時終了）

| 条件 | 例外 |
|---|---|
| スキーマYAMLのフォーマット違反 | `pydantic.ValidationError` |
| `source_dir`が存在しない / プロジェクト外 | `pydantic.ValidationError` |
| `database_path`の拡張子不正 | `pydantic.ValidationError` |
| FK参照先テーブルが未定義 | `ValueError` |
| 循環依存 | `ValueError`（`CycleError`をラップ） |
| `schema_dir`にYAMLが0件 | `FileNotFoundError` |
| リレーション定義が未定義テーブル/カラムを参照 | `ValueError` |

### 実行時エラー（レポートに記録して継続）

| 条件 | 記録先 | 挙動 |
|---|---|---|
| INSERTエラー | `LoadError` | 当該ファイルをNGとしてレポートに記録 |
| checksクエリエラー | `CheckResult(status=CheckStatus.ERROR)` | エラー内容をmessageに記録 |
| プロファイリングエラー | `ColumnProfile(error=...)` | エラー内容をerrorフィールドに記録 |
| リレーションチェックSQL実行エラー | `CheckResult(status=CheckStatus.ERROR)` | エラー内容をmessageに記録 |
| ロードエラーのあるテーブルを含むリレーション | `CheckResult(status=CheckStatus.SKIPPED)` | スキップ理由をmessageに記録 |

---

## 11. HTMLレポート仕様

### 全体構成

```
[サマリーセクション]
  - 実行日時・対象DBパス
  - 総テーブル数 / OK件数 / NG件数
  - エクスポート実行有無（--exportフラグがあった場合のみ表示）

[テーブル別セクション（テーブル数分繰り返し）]
  - テーブル名・ステータス（OK/NG）
  - ファイルロード結果
  - ロジックバリデーション結果（checks）
  - 集計バリデーション結果（aggregation_checks）
  - 基本統計量テーブル
  - エクスポート結果（export_resultがNoneでない場合のみ）

[リレーションカーディナリティ検証セクション（relations_path設定時のみ）]
  - リレーション検証サマリー（総数 / OK / NG / SKIPPED）
  - 各リレーションチェック結果
```

### ステータス表示

| ステータス | 表示 |
|---|---|
| OK | ✅ |
| NG | ❌ |
| ERROR | ❌ |
| SKIPPED | ⚠️ |

---

## 12. 実装順序

依存関係に従い以下の順で実装・動作確認すること。

1. **`logger.py`**
2. **`parser.py`** + **`tests/test_parser.py`**
3. **`builder.py`** + **`tests/test_builder.py`**
4. **`loader.py`** + **`tests/test_loader.py`**
5. **`checker.py`** + **`tests/test_checker.py`**
6. **`relation.py`** + **`tests/test_relation.py`**
7. **`profiler.py`** + **`tests/test_profiler.py`**
8. **`exporter.py`** + **`tests/test_exporter.py`**
9. **`reporter.py`** + テンプレート + **`tests/test_reporter.py`**
10. **`main.py`** + **`tests/test_integration.py`**
11. **`init.py`**
12. **`cli.py`**

---

## 13. 制約・既知の限界

| 制約 | 内容 |
|---|---|
| `.xls`非対応 | DuckDBの`read_xlsx`は`.xlsx`のみ対応 |
| DuckDBエラーパース | エラーメッセージ文字列はバージョンアップで変更される可能性あり。`raw_message`でフォールバック |
| 高カーディナリティ列 | `COUNT(DISTINCT col)`は列ごと実行のため、カラム数×テーブルサイズに比例 |
| 数値型統計量の実行時間 | 数値型列ごとに個別SQL実行 |
| checksクエリのSQL任意実行 | 内部ツールとして許容。外部公開時は再検討 |
| マルチスキーマ非対応 | DuckDBの`main`スキーマのみ |
| リレーション検証の複合キー | 全カラム一致を要求。部分一致は未対応 |
| リレーション検証のNULL処理 | NULLは参照整合性チェックから除外（SQL FK標準） |

---

## 14. エラーハンドリング規約

本プロジェクトでは、エラーの発生箇所に応じて2つのハンドリング方針を使い分ける。

### 入力バリデーション（即時例外）

対象モジュール: `parser.py`（スキーマYAML）、`main.py`（`ProjectConfig`による設定ファイル検証）

- `ValueError`、`FileNotFoundError` 等の例外を送出し、パイプラインを即時停止する
- 理由: 設定やスキーマの誤りはプログラマ（利用者）のミスであり、早期に検出して修正を促すべきである（フェイルファスト原則）

### ランタイム操作（Result オブジェクト返却）

対象モジュール: `loader.py`（`LoadError`）、`checker.py`（`CheckResult`）、`exporter.py`（`ExportResult`）、`profiler.py`（`ColumnProfile.error`）

- 例外を送出せず、構造化されたResultオブジェクトにエラー情報を格納して返却する
- パイプラインは停止せず、全テーブル・全チェックを処理した後にHTMLレポートでまとめて報告する
- 理由: データ品質の問題は入力データに起因するものであり、可能な限り多くの問題を一度に検出・報告することが利用者の効率につながる
