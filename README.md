Разработка key-value blob storage для временных рядов.
Ссылки
* https://docs.victoriametrics.com/keyConcepts.html
* https://docs.victoriametrics.com/Single-server-VictoriaMetrics.html#deduplication
* https://prometheus.io/docs/prometheus/latest/storage/
* https://bytedance.feishu.cn/docs/doccnZmYFqHBm06BbvYgjsHHcKc - TerarkDB forked from RocksDB
* https://www.influxdata.com/
* http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
* https://github.com/facebookarchive/beringei
* http://daslab.seas.harvard.edu/monkey/
* [Лекции по таймсериям, обзор БД](https://db.cs.cmu.edu/seminar2017/)

Интерфейс:
* https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_keyvalue.proto

Базовая цель на сезон (20 ноября)
1. Берем интерфейс таблетки.
1. Реализуем по ней read/write.
1. Добавляем политики мержей.

Плюсом будет (10 - 17 декабря)
1. Интегрировать с Java Panama.
1. Добавить расчет агрегатов.

Предварительно
1. Ознакомиться с другими системами
1. Выбрать модель хранения структуры данных

_Политики слияния_ (выносятся как настройки для временного ряда):
После накопления N часов прорядить функцией f с сеткой T секунд. Т.е.все значения в окне T секунд проходят через функцию f и остается одно.
Примеры функций: moving average, max, min, last, count, sum, weighted average, rate…
Учитывать, что прореживать можно несколькими функциями.

_Агрегаты_ - это функции, применяемые к нескольким временным рядам. Например, для всех рядов x1, x2,.., xn посчитать сумму и записать в ряд xSum. Тут соответственно стоит использовать векторные вычисления.
Примеры функций: sum, avg, count, min, max, correlation.

Примеры политики прореживания:
* пятиминутная сетка спустя одну неделю (значение по умолчанию);
* минутная сетка спустя один месяц, пятиминутная сетка спустя два месяца;
* минутная сетка спустя один месяц, пятиминутная сетка спустя три месяца;
* пятиминутная сетка спустя два месяца;
* сохранять исходные значения (не прореживать данные).


