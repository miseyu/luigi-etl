# luigi-etl

* luigiで作ったETL用ツール

## 使用言語

* [Python2.7](https://www.python.org)

## 主要ライブラリ

* [Luigi](https://github.com/spotify/luigi)
* [apache-beam](https://github.com/apache/incubator-beam/tree/python-sdk/sdks/python)

## 使用ツール

* pip

## 起動方法

### ローカル起動
``` sh
$ python tasks.py MyQuery --local-scheduler
```

### 環境変数

* GCS_TOUCH_VERSION: TOUCHのバージョン
* STREAM_ID: ストリームモードの場合のID
* BEAM_WORKER_COUNT: Dataflowのワーカ数

### central起動
``` sh
$ luigid --port 8082
```
### centralに投げる
``` sh
$ python tasks.py MyQuery
```
