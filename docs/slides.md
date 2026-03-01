---
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Helvetica Neue', 'Hiragino Sans', 'Noto Sans JP', sans-serif;
  }
  h1 {
    color: #2d3748;
  }
  h2 {
    color: #2d3748;
    border-bottom: 2px solid #4299e1;
    padding-bottom: 8px;
  }
  table {
    font-size: 0.75em;
  }
  code {
    font-size: 0.85em;
  }
  pre {
    font-size: 0.75em;
  }
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1em;
  }
---

<!-- _class: lead -->
<!-- _paginate: false -->

# **tval**

### YAML駆動のテーブルデータバリデーター
#### DuckDBで実現する宣言的データ品質検証

<br>

`pip install tval-cli`

---

## Agenda

1. **課題**: 分析前データバリデーションで何を確認すべきか
2. **既存手法の限界**: なぜ既存ツールでは不十分か
3. **Why DuckDB**: 制約エンジンとしてのDuckDB
4. **アーキテクチャ**: パイプライン設計とモジュール構成
5. **設計判断 Deep Dive**: 6つの技術的意思決定
6. **テスト戦略**: モック不使用の理由
7. **まとめ**

---

## 分析前バリデーションの6階層モデル

| 階層 | 検証内容 | 例 |
|:---:|---|---|
| **1. ファイル・物理層** | 存在・形式・文字コード | 納品漏れ、Shift-JIS |
| **2. スキーマ・構造層** | カラム名・数・データ型 | 型不一致、列名typo |
| **3. 単一カラム制約層** | NOT NULL・許容値・値範囲 | 必須項目が空 |
| **4. 複数カラム制約層** | PK・UNIQUE・カラム間条件 | `開始日 > 終了日` |
| **5. テーブル間制約層** | FK・参照整合性・カーディナリティ | 孤立レコード |
| **6. 集計・統計層** | 件数・合計値・分布・NULL率 | 前月比10倍の売上 |

> **下層が崩れると上層の検証は意味を成さない** &mdash; この順序が重要

---

## 現状の課題

### 分析前バリデーションの3つの問題

**属人化** &mdash; 分析者ごとにバリデーションの品質・カバレッジが異なる

**時間コスト** &mdash; 手作業でのデータ品質チェックが分析のボトルネックに

**偽りの安心** &mdash; クラウドDWHでスキーマ定義しても制約は**強制されない**

---

## 既存アプローチの限界

| アプローチ | 手法 | 主な問題 |
|---|---|---|
| **pandas / polars** | 個別スクリプト | 属人化・メモリ制約・網羅性の欠如 |
| **great-expectations 等** | 専用フレームワーク | 学習コスト高・フレームワークロックイン |
| **クラウドDWH + SQL** | BigQuery / Redshift 等 | **制約が宣言できるが強制されない** |

<br>

> 3番目のパターンが最も危険: スキーマが*正しく見える*が、不正データが素通りする

---

## 制約エンジン比較: 核心の問題

| 制約 | DuckDB | PostgreSQL | BigQuery | Redshift | Snowflake |
|---|:---:|:---:|:---:|:---:|:---:|
| Type enforcement | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| NOT NULL | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| **PRIMARY KEY** | :white_check_mark: | :white_check_mark: | :warning: | :warning: | :warning: |
| **FOREIGN KEY** | :white_check_mark: | :white_check_mark: | :warning: | :warning: | :warning: |
| **CHECK** | :white_check_mark: | :white_check_mark: | :warning: | :warning: | :warning: |

:warning: = 宣言可能だが**未強制**（違反データがINSERTされても無視）

> 分散DWHは水平スケールと引き換えに制約強制を犠牲にしている

---

## Why DuckDB

| 要件 | DuckDB | PostgreSQL | Cloud DWH |
|---|:---:|:---:|:---:|
| 制約強制（PK/FK/CHECK） | :white_check_mark: | :white_check_mark: | :x: |
| ファイル直接読み込み | :white_check_mark: | :x: | :white_check_mark: |
| サーバーレス・組込み型 | :white_check_mark: | :x: | :x: |
| OLAP・列指向ストレージ | :white_check_mark: | :x: | :white_check_mark: |
| 一時的な検証用途に適する | :white_check_mark: | :x: | :x: |

<br>

**tvalの原則**: 制約強制はDuckDBに委譲し、残りはユーザー定義SQLでカバー

---

## tvalのアプローチ: 6階層への対応

| 階層 | tvalの対応方式 |
|:---:|---|
| 1. ファイル・物理層 | `loader.py` &mdash; chardetエンコーディング検出 + DuckDB read_* |
| 2. スキーマ・構造層 | DuckDB `CREATE TABLE` + `INSERT` による型強制 |
| 3. 単一カラム制約層 | DuckDB NOT NULL制約 + `allowed_values` 自動SQLチェック |
| 4. 複数カラム制約層 | DuckDB PK/UNIQUE制約 + ユーザー定義 `checks` SQL |
| 5. テーブル間制約層 | DuckDB FK制約 + `relations.yaml` カーディナリティ検証 |
| 6. 集計・統計層 | `aggregation_checks` SQL + `profiler.py` 統計算出 |

> YAML定義 = コードでなく**データ** &rarr; レビュー可能・差分管理・属人化解消

---

## 処理パイプライン

```
config.yaml ─┐    schema/*.yaml ─┐    relations.yaml ─┐
             │                   │                    │
             └───── Pydantic バリデーション ───────────┘
                         │
                 DAGによるロード順決定
                 (FK依存関係を解決)
                         │
                 DuckDBテーブル CREATE
                 (PK/FK/UNIQUE/NOT NULL制約付き)
                         │
         ┌── conn_rw ────┤
         │   ファイルINSERT(CSV/XLSX/Parquet)
         │               │
         └── conn_ro ────┤
             ロジック検証 → リレーション検証 → プロファイリング
                         │
                 Parquetエクスポート (All-or-Nothing)
                         │
                 HTMLレポート生成
```

---

## モジュール構成

| モジュール | 行数 | 責務 |
|---|---:|---|
| `parser.py` | 234 | YAML解析 + Pydanticモデル（14クラス） |
| `builder.py` | 107 | DDL生成 + トポロジカルソート |
| `loader.py` | 395 | CSV/XLSX/Parquet → DuckDB + エンコーディング検出 |
| `checker.py` | 169 | ロジック・集計バリデーション実行 |
| `relation.py` | 291 | テーブル間カーディナリティ検証 |
| `profiler.py` | 215 | カラム統計量算出 |
| `exporter.py` | 78 | Parquetエクスポート |
| `reporter.py` | 88 | Jinja2 HTMLレポート生成 |
| `main.py` | 176 | パイプラインオーケストレーション |

**合計 ~2,000行** / テスト ~1,700行 / ドキュメント ~1,100行

---

## YAML定義の例

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
  - name: status
    logical_name: ステータス
    type: VARCHAR
    not_null: true
    allowed_values: ["pending", "shipped", "cancelled"]
table_constraints:
  primary_key:
    columns: [order_id]
  foreign_keys:
    - columns: [user_id]
      references: { table: users, columns: [user_id] }
  checks:
    - description: 合計は小計+税と一致
      query: "SELECT COUNT(*) FROM {table} WHERE total != sub_total + tax"
  unique: []
  aggregation_checks: []
```

---

<!-- _class: lead -->

# 設計判断 Deep Dive

6つの技術的意思決定を掘り下げる

---

## 設計判断 1: Fail-Fast vs Result Objects

### エラーハンドリングの二重戦略

| フェーズ | 戦略 | 理由 |
|---|---|---|
| **起動時**（YAML不正・循環依存） | 即時例外（Fail-Fast） | 設定ミスは**開発者のバグ**。早期検出 |
| **実行時**（データ品質問題） | Result Objects | データ問題は**一度に全て**報告すべき |

```python
# 起動時: 即座にValueError
if ref_table not in name_to_def:
    raise ValueError(f"FK reference table is not defined: ...")

# 実行時: エラーを収集して継続
error = parse_duckdb_error(file_path, str(e))
errors.append(error)  # パイプラインは止めない
```

> 4種のResult型: `LoadError`, `CheckResult`, `ExportResult`, `ColumnProfile`

---

## 設計判断 2: DuckDBへの委譲とコネクション分離

### 構造チェック = DuckDBの制約機能に完全委譲

```python
# builder.py: DDLに制約を埋め込む → DuckDBが INSERT 時に強制
def build_create_table_sql(tdef: TableDef) -> str:
    col_defs = (
        f'    {quote_identifier(col.name)} {col.type}'
        + (" NOT NULL" if col.not_null else "")
        for col in tdef.columns
    )
    # PK, UNIQUE, FK 制約も同様に生成
```

### コネクション分離で安全性を確保

| 接続 | 用途 | 理由 |
|---|---|---|
| `conn_rw` | データロード（INSERT） | 書き込みが必要 |
| `conn_ro` | 検証・プロファイル・エクスポート | **ユーザー定義SQLによるデータ破壊を防止** |

---

## 設計判断 3: トポロジカルソートによるFK依存解決

```python
# builder.py: graphlib.TopologicalSorter でDAGをソート
def build_load_order(table_defs: list[TableDef]) -> list[TableDef]:
    graph: dict[str, set[str]] = {t.table.name: set() for t in table_defs}
    for tdef in table_defs:
        for fk in tdef.table_constraints.foreign_keys:
            graph[tdef.table.name].add(fk.references.table)

    sorter: TopologicalSorter[str] = TopologicalSorter(graph)
    try:
        ordered_names = list(sorter.static_order())
    except CycleError as e:
        raise ValueError(f"Circular dependency detected: {e.args[1]}")
    return [name_to_def[name] for name in ordered_names]
```

- FK依存をDAGとして表現 → 参照先テーブルを先にCREATE & INSERT
- `graphlib`はPython 3.9+の標準ライブラリ（外部依存なし）
- 循環依存は即時検出 → Fail-Fast

---

## 設計判断 4: SQLインジェクション対策（2層防御）

### 第1層: 正規表現ホワイトリスト

```python
# builder.py
def validate_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

def quote_identifier(name: str) -> str:
    return f'"{validate_identifier(name)}"'  # 第2層: ダブルクォート
```

### 値のバインド: パラメータプレースホルダ

```python
# checker.py: allowed_values は ? でバインド
query = (f'SELECT COUNT(*) FROM {{table}} '
         f'WHERE {qcol} NOT IN (SELECT UNNEST(?::VARCHAR[])) '
         f'AND {qcol} IS NOT NULL')
conn.execute(query, [col.allowed_values])
```

### ユーザー定義SQL: read-only接続で実行 → データ破壊を構造的に防止

---

## 設計判断 5: CSV文字コード処理の最適化

```python
# loader.py
def _resolve_csv_path(file_path, confidence_threshold):
    _CHARDET_SAMPLE_SIZE = 8192  # 先頭8KBのみサンプリング
    with open(file_path, "rb") as f:
        sample = f.read(_CHARDET_SAMPLE_SIZE)

    detected = chardet.detect(sample)
    encoding = detected.get("encoding") or "utf-8"
    confidence = detected.get("confidence") or 0.0

    if confidence < confidence_threshold:
        raise EncodingDetectionError(...)   # 信頼度不足 → エラー記録

    if encoding.lower().replace("-", "") in ("utf8", "ascii"):
        return file_path, False             # パススルー（コピー不要）

    # 非UTF-8 → UTF-8一時ファイルに変換
    tmp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", ...)
    with open(file_path, "r", encoding=encoding) as src, tmp:
        shutil.copyfileobj(src, tmp)
    return tmp.name, True
```

> UTF-8ファイルはゼロコピー。非UTF-8のみストリーミング変換

---

## 設計判断 6: テーブル間カーディナリティ検証

### `relations.yaml` で宣言的にリレーションを定義

```yaml
relations:
  - name: users-orders
    cardinality: "1:N"
    from: { table: users, columns: [user_id] }
    to:   { table: orders, columns: [user_id] }
```

### カーディナリティ別の自動SQL生成

| 種別 | チェック内容 | 数 |
|---|---|---:|
| **1:1** | 両側一意性 + 双方向参照整合性 | 4 |
| **1:N** | 1側一意性 + N→1参照整合性 | 2 |
| **N:1** | 1側一意性 + N→1参照整合性 | 2 |
| **N:N** | 双方向参照整合性 | 2 |

> FK制約だけでは検証できない「カーディナリティ」を宣言的に検証

---

## テスト戦略: モック不使用の理由

### 全テスト（~1,700行）で実DuckDBインスタンスを使用

```
tests/
├── test_builder.py     # CREATE TABLE SQL生成 + トポロジカルソート
├── test_checker.py     # allowed_values / checks / aggregation_checks
├── test_exporter.py    # Parquetエクスポート
├── test_integration.py # E2Eパイプライン
├── test_loader.py      # CSV/XLSX/Parquet ロード + エラーパース
├── test_parser.py      # Pydanticバリデーション
├── test_profiler.py    # 統計量算出
├── test_relation.py    # カーディナリティ検証
└── test_reporter.py    # HTMLレポート生成
```

**なぜモックしないか**:
- DuckDBのSQL方言・制約挙動がバリデーションの**核心**
- モックすると「DuckDBが実際にどう振る舞うか」のテストにならない
- DuckDBはインプロセスDB → テスト速度は十分高速

---

## プロジェクト概要

<div class="columns">
<div>

### 技術スタック

| 項目 | 内容 |
|---|---|
| 言語 | Python 3.10 - 3.13 |
| DB | DuckDB (インプロセス) |
| スキーマ検証 | Pydantic |
| テンプレート | Jinja2 |
| 文字コード検出 | chardet |
| 静的解析 | ruff + mypy strict |
| CI | GitHub Actions |

</div>
<div>

### 規模

| 項目 | 行数 |
|---|---|
| ソースコード | ~2,000行 |
| テストコード | ~1,700行 |
| ドキュメント | ~1,100行 |
| 依存ライブラリ | 5個のみ |

### 対応フォーマット
- CSV（文字コード自動検出）
- Excel（.xlsx）
- Parquet

</div>
</div>

---

## まとめ: tvalの3つの設計原則

### 1. DuckDB制約エンジンへの委譲
構造チェック（型・NULL・PK・FK・UNIQUE）を自前実装しない
&rarr; 信頼性の高いDBエンジンに任せる

### 2. 宣言的YAML定義による標準化
バリデーションロジックをコードではなく**データ**として定義
&rarr; レビュー可能・差分管理・属人化解消

### 3. Fail-Fast + Result Objectsの二重戦略
設定ミスは即時停止、データ品質問題は一度に全て報告
&rarr; 開発者体験とユーザー体験の両立

---

<!-- _class: lead -->
<!-- _paginate: false -->

# Thank you

`pip install tval-cli`

<br>

ご質問・フィードバックをお待ちしています
