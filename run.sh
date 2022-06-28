#!/bin/bash

cd /root/apps/keepa || exit
# Указываем полный путь до pipenv, чтобы crontab его нашёл
/usr/local/bin/pipenv run python /root/apps/keepa/keepa/run.py