# keepa
### Вспомогательный сервис для загрузки файлов в s3 хранилище

Работает как обычный скрипт. Изначально предназначен для использования с crontab.

Загружает файлы из указанных директорий в s3 хранилище.
Может грузить как одному файлу, так и всю директорию архивом.
Поддерживает добавление дат к загружаемым файлам.

Пример конфига с пояснениями - `config.toml.example`. 
Из этого примера необходимо создать фактический конфиг с именем `config.toml`.

### Режим оптимизации хранения по датам

После загрузки файлов в хранилище, скрипт удаляет файлы 
за прошлые месяцы, оставляя только один, самый актуальный.

Допустим за прошлый месяц в хранилище лежат файлы:
```shell
testfile-25-05-2020.txt
testfile-26-05-2020.txt
testfile-27-05-2020.txt
testfile-28-05-2020.txt
```

После обработки в хранилище останется только один файл:

```shell
testfile-28-05-2020.txt
```

И так по каждому месяцу, кроме текущего.