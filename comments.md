Саша, привет.
Спасибо за ревью, видно, что ты реально читал весь код)
Здесь комментарии только по вопросам или по тому, что я не сделала. Остальные замечания я устранила (полагаю).
NB: так как все запросы sql я положила к dag в папку sql, папка migrations теперь пустая.

**Этап 0**
Я завернула создание таблиц в staging и mart в отдельный даг etl_tables_migration, хотя мне кажется, это излишне. Мне кажется, достаточно запустить скрипты, ведь это не повторяющийся процесс.  

**Этап 1**  
[min_threshold=5, max_threshold=1000000] - минимум из примера в следующем спринте, максимум не планировала ставить, но без него нельзя, выбрала просто большое значение.

Вот это вот оказалось интересно:
*"какая-то странная конструкция — функция с оператором внутри 🤔 а почему не просто PostgresOperator?"*
Таск с PostgresOperator у меня отрабатывал в самом начале самого дага (я не включала его в последовательность выполнения тасков). А после rows_check_task (то есть после проверки, когда он должен записывать результаты проверки) выдавал ошибку:
" Marking task as SUCCESS. dag_id=etl_update_user_data_project, task_id=rows_check_task.check_rows_order_log, execution_date=20220626T132532, start_date=20220626T132738, end_date=20220626T132739
[2022-06-26, 13:27:39 UTC] {local_task_job.py:156} INFO - Task exited with return code 0
[2022-06-26, 13:27:39 UTC] {taskinstance.py:1740} ERROR - Error when executing on_success_callback
Traceback (most recent call last):
  File "/usr/local/lib/python3.8/dist-packages/airflow/models/taskinstance.py", line 1738, in _run_finished_callback
    task.on_success_callback(context)
TypeError: 'PostgresOperator' object is not callable "

Но вариант через функцию тоже оказался нерабочим. Я нашла похожую проблему https://stackoverflow.com/questions/67229594/airflow-running-postgresoperator-within-a-pythonoperator-with-taskflow-api, где предполагается, что PostgresOperator не видит пусть до query.sql. Но я не смогла заставить его работать, даже прописывая команду. Нашла рабочее решение. Единственное, мне не нравится, что нужно вызывать четыре разные функции (по две на каждую таблицу, на случай failure / success), но у меня не получилось передавать в одну и ту же функцию разные значения в зависимости от failure / success.


**Этап 2**  
Я заменила представление на таблицу, но не меняла period_id (у меня состоит из года и месяца вместе). Я поняла, что это не критичное замечение, и мне кажется удобным, что год и месяц периода указаны в одном столбце.

