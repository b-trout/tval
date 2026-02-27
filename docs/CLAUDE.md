# CLAUDE.md

## このファイルの目的

このリポジトリは「分析前受領データのバリデーションツール」のPoC実装である。
実装の全仕様は `DESIGN.md` に記載されている。本ファイルはClaude Codeが実装を行う際の
作業指針を定めるものであり、`DESIGN.md` と矛盾する場合は `DESIGN.md` を優先すること。

---

## 実装前に必ずやること

1. `DESIGN.md` を最初から最後まで読む
2. 不明点があれば実装を始める前に質問する。推測で実装しない
3. Before task starts, create new branch from main branch
---

## 実装順序

`DESIGN.md` セクション12に定義された順序を厳守すること。順序を変えない。

```
1.  logger.py
2.  parser.py + tests/test_parser.py
3.  builder.py + tests/test_builder.py
4.  loader.py + tests/test_loader.py
5.  checker.py
6.  profiler.py
7.  exporter.py
8.  reporter.py + templates/report.html.j2
9.  main.py + tests/test_integration.py
10. init.py
11. cli.py
12. pyproject.toml
```

各モジュールを実装したら、次のモジュールに進む前に単体で動作確認すること。
全モジュールを一度に実装しない。

---

## 技術的制約

### Python バージョン
3.10以上必須

### 依存ライブラリ
以下のみ使用する。新たな依存を追加しない。

```
duckdb
pydantic
pyyaml
jinja2
```

### パッケージ構成
`src/`レイアウトを採用する。全モジュールは `src/tval/` 以下に配置する。

### 禁止事項
- `openpyxl` の使用禁止（DuckDBの`read_xlsx`で代替）
- 文字コード検出ライブラリは`chardet`のみ使用。`charset-normalizer`等の代替ライブラリ使用禁止
- `SUMMARIZE` の使用禁止（`profiler.py`は独自SQL実装。`DESIGN.md` セクション8.5参照）
- `Optional[X]` を `TableConstraints` のフィールドに使用禁止（全フィールド必須の `list[X]`）
- `click` 等の外部CLIライブラリの使用禁止（`argparse`で実装）
- `print()` の使用禁止（`tval init`の完了メッセージを除く。ログは`get_logger()`経由）
- `tval init`で既存ディレクトリへの上書き禁止
- `checker.py`・`profiler.py`へのread/write接続(`conn_rw`)の受け渡し禁止。必ずread_only接続(`conn_ro`)を渡すこと
- `allowed_values`チェックをユーザーに手書きさせる実装禁止。`checker.py`が`ColumnDef.allowed_values`から自動生成すること
- `DATETIME_TYPES`を`loader.py`と`profiler.py`で二重定義禁止。どちらかに定義してもう一方でインポートすること
- `format`指定カラムのINSERTで`SELECT *`を使用禁止。`_build_insert_select()`で明示的なSELECTを生成すること
- `database_path`の拡張子検証をスキップする実装禁止
- `source_dir`のパス正規化・プロジェクトルート確認をスキップする実装禁止

---

## コーディング規約

### 型アノテーション
全関数に型アノテーションを付ける。戻り値型も省略しない。

### 識別子のクォート
SQLに埋め込む全識別子（テーブル名・カラム名）は `quote_identifier()` を経由すること。
`builder.py` に実装し、他モジュールは `from .builder import quote_identifier` でインポートする。
直接文字列フォーマットで識別子を埋め込まない。

### 値のバインド
SQLの値は必ず `?` プレースホルダでバインドする。

```python
# 正しい
conn.execute("SELECT * FROM read_csv_auto(?)", [file_path])

# 禁止
conn.execute(f"SELECT * FROM read_csv_auto('{file_path}')")
```

### ロギング
`print()` を使用禁止。全てのログ出力は `validator/logger.py` の `get_logger(__name__)` を使うこと。
ログのフォーマット・出力先・extraフィールドの要件は `DESIGN.md` セクション4.1を参照。


`LoadError.raw_message` は必ず保持する。パースが失敗してもこのフィールドは空にしない。

---

## コード品質

### 静的解析・型チェック

コミット前に以下が全て通ること。1つでも失敗したらコミットしない。

```bash
ruff check src/ tests/
ruff format --check src/ tests/
mypy src/
```

エラーが出た場合は修正してから再実行すること。`# noqa`・`# type: ignore`による握り潰しは原則禁止。

### pre-commit フック

`pre-commit` フレームワークにより上記チェックを `git commit` 時に自動実行する。
初回セットアップ:

```bash
uv sync --extra dev
uv run pre-commit install
```
どうしても必要な場合はインラインコメントで理由を明記すること。

### `pyproject.toml`への設定追加

ruffとmypyの設定を`pyproject.toml`に追記すること。

```toml
[project.optional-dependencies]
dev = [
    "pytest",
    "ruff",
    "mypy",
]

[tool.ruff]
target-version = "py39"
line-length = 88

[tool.ruff.lint]
select = ["E", "F", "I"]   # pycodestyle, pyflakes, isort

[tool.mypy]
python_version = "3.10"
strict = true
```



テストフレームワークは`pytest`を使用する。`tests/`ディレクトリに配置する。
モックは極力使わず、実際のデータ・実際のDuckDBインスタンスを使って検証すること。

### 単体テスト対象

以下のモジュールに限り単体テストを実装すること。それ以外は不要。

**`tests/test_parser.py`**
- 正常系: 有効なYAMLからTableDefが生成されること
- 異常系:
  - `table_constraints`の省略でValidationError
  - `source_dir`が存在しないパスでValidationError
  - `source_dir`にプロジェクトルート外のパス（`../../`）を指定でValidationError
  - `primary_key`に存在しないカラム名を指定でValidationError
  - `export.partition_by`に存在しないカラム名を指定でValidationError
  - `allowed_values`が指定されたカラムが正しくパースされること
  - `format`がDATE/TIMESTAMP/TIME以外の型に指定された場合にValidationError

**`tests/test_builder.py`**
- 正常系: FK依存関係が正しいトポロジカル順序で返ること
- 異常系:
  - 循環依存でValueError（エラーメッセージに関係テーブル名が含まれること）
  - FK参照先テーブルが未定義でValueError

**`tests/test_loader.py`**（エラーパースのみ。実DBへのINSERTは不要）
- 各`error_type`のDuckDBエラー文字列を直接`parse_duckdb_error()`に渡し、正しい`error_type`・`column`・`row`が返ること
- 未知のエラーメッセージが`UNKNOWN`として返り、`raw_message`が保持されること
- `_resolve_csv_path()`に信頼度が閾値未満のファイルを渡した場合に`EncodingDetectionError`が送出されること

### 結合テスト

**`tests/test_integration.py`**

正常系のエンドツーエンドテストを1本のみ実装する。

- `tests/fixtures/`に最小限のテスト用CSVとスキーマYAMLを用意する
- `run(config_path)`を呼び出し、`report.html`が生成されることを確認する
- `tval run --export`相当の`run(config_path, export=True)`を呼び出し、Parquetが生成されることを確認する

### 完了条件への追加

`pytest`で全テストが通ることを完了条件に含める。



`DESIGN.md` に記載のない実装上の判断（ログ出力の形式、テストコードの有無など）は
実装者の裁量に委ねる。ただし以下は必ず守ること。

- フェイルファスト原則：起動時エラーは即時終了（`DESIGN.md` セクション10参照）
- 実行時エラーはレポートに記録して継続（同上）
- `DESIGN.md` セクション13「制約・既知の限界」に記載の内容はスコープ外。実装しない

---

## 完了条件

以下が全て満たされた状態を実装完了とする。

- [ ] `pytest` で全テストが通る
- [ ] `ruff check src/ tests/` がエラーなし
- [ ] `ruff format --check src/ tests/` がエラーなし
- [ ] `mypy src/` がエラーなし
- [ ] `pip install -e .[dev]` が正常終了する
- [ ] `tval init` で `tval/` スケルトンが生成される
- [ ] `tval init` で `.gitignore` に `tval/data/`, `tval/output/` が追記される
- [ ] `tval init` を2回実行しても既存ディレクトリへの上書きが発生しない
- [ ] `tval run` が `./tval/config.yaml` を自動探索して実行される
- [ ] `tval run --config PATH` で指定パスの`config.yaml`を使って実行される
- [ ] `database_path`に`.duckdb`以外の拡張子を指定した場合に即時エラーで終了する
- [ ] `source_dir`にプロジェクトルート外のパス（`../../`等）を指定した場合にバリデーションエラーになる
- [ ] `output/report.html` が生成される
- [ ] ロードエラーが発生したファイルについて、エラー種別と `raw_message` がレポートに表示される
- [ ] `checks` / `aggregation_checks` の結果がOK/NG/SKIPPEDで表示される
- [ ] `allowed_values`が指定されたカラムについて、許容値外のデータがあればNGとしてレポートに表示される
- [ ] `format`が指定されたDATE/TIMESTAMP/TIMEカラムについて、指定フォーマットで正しくパースされてロードされる
- [ ] `format`をDATE/TIMESTAMP/TIME以外の型に指定した場合にYAML読み込み時にValidationErrorになる
- [ ] 数値型カラムの統計量（件数・not_null件数・ユニーク件数・平均・標準偏差・歪度・尖度・パーセンタイル）がレポートに表示される
- [ ] 非数値型カラムの統計量（件数・not_null件数・ユニーク件数）がレポートに表示される
- [ ] `tval run --export` で全テーブルOKの場合にParquetが `tval/output/parquet/{table_name}/` に書き出される
- [ ] `partition_by`指定時にHive形式のディレクトリ構造でParquetが書き出される
- [ ] `partition_by`未指定時に `{table_name}.parquet` 単一ファイルで書き出される
- [ ] 1テーブルでもNGがある状態で `tval run --export` を実行した場合、全テーブルのエクスポートがSKIPPEDになる
- [ ] エクスポート結果（OK/SKIPPED/ERROR）がHTMLレポートに表示される
