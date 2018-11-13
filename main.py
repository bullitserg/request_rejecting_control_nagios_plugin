import re
import json
import argparse
from itertools import count
from sys import exit as s_exit
from transliterate import translit
from ets.ets_mysql_lib import MysqlConnection as Mc

PROGNAME = 'Check request status after blocking Nagios plugin'
DESCRIPTION = '''Плагин Nagios для выполнения запросов (с возможностью сброса ошибок по дате)'''
VERSION = '1.0'
AUTHOR = 'Belim S.'
RELEASE_DATE = '2018-06-20'

OK, WARNING, CRITICAL, UNKNOWN = range(4)

DEFAULT_WARNING_LIMIT = 1
DEFAULT_CRITICAL_LIMIT = 1
DEFAULT_DATA_SEPARATOR = ' | '

get_requests_info_query = '''SELECT
  p.registrationNumber AS registration_number,
  p.requestEndGiveDateTime AS request_end_give_datetime,
  r.id AS r_id,
  r.requestStatusId AS request_status_id,
  o.inn,
  o.kpp,
  o.ogrn,
  s.title AS r_status_title,
  peo.additional_information
FROM procedures p
  JOIN procedureRequest r
    ON r.procedureId = p.id
    AND r.requestStatusId != 19
  JOIN organization o
    ON o.id = r.organizationId
  JOIN procedureStatus s
    ON s.id = r.requestStatusId
  JOIN payment_edo_operation peo
    ON SUBSTR(peo.additional_information, INSTR(peo.additional_information, '{"app_id":"') + 31, 7) = r.id
    AND peo.type_id = 3
WHERE p.actualId IS NULL
AND p.requestEndGiveDateTime BETWEEN SUBDATE(NOW(), INTERVAL 1 HOUR) AND NOW()
AND peo.created_at BETWEEN SUBDATE(NOW(), INTERVAL 1 HOUR) AND NOW()
;'''

error_text = 'Found %s new errors:'
no_error_text = 'Errors not found'
error_data = []
error_count = 0
error_counter = count(start=1)


# обработчик параметров командной строки
def create_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument('-v', '--version', action='store_true',
                        help="Показать версию программы")

    parser.add_argument('-w', '--warning_limit', type=int, default=DEFAULT_WARNING_LIMIT,
                        help="Лимит для срабатывания WARNING (по умолчанию %s)" % DEFAULT_WARNING_LIMIT)

    parser.add_argument('-c', '--critical_limit', type=int, default=DEFAULT_CRITICAL_LIMIT,
                        help="Лимит для срабатывания CRITICAL (по умолчанию %s)" % DEFAULT_CRITICAL_LIMIT)

    parser.add_argument('-p', '--data_separator', type=str, default=DEFAULT_DATA_SEPARATOR,
                        help="Разделитель вывода данных(по умолчанию '%s')" % DEFAULT_DATA_SEPARATOR)

    return parser


def show_version():
    print(PROGNAME, VERSION, '\n', DESCRIPTION, '\nAuthor:', AUTHOR, '\nRelease date:', RELEASE_DATE)


if __name__ == '__main__':
    try:
        # парсим аргументы командной строки
        my_parser = create_parser()
        namespace = my_parser.parse_args()

        # вывод версии
        if namespace.version:
            show_version()
            s_exit(OK)

        cn = Mc(connection=Mc.MS_44_1_CONNECT).connect()
        requests_info = cn.execute_query(get_requests_info_query, dicted=True)

        for request_info in requests_info:
            additional_information = json.loads(request_info['additional_information'])
            additional_information['status'] = int(additional_information['status'])
            additional_information['status_description'] = additional_information.get('status_description', '')

            # если статус 0, то просто ответ, в сраку его
            if additional_information['status'] == 0:
                continue

            # если деньги заблокированы и заявка принята (ну или какая то там чудо блокировка), то все ок
            if request_info['request_status_id'] in [20, 74] and additional_information['status'] in [1, -10]:
                continue

            # если не заблокированы, то отклонена, все ок
            if request_info['request_status_id'] == 69 \
                    and additional_information['status'] == 2 \
                    and re.findall(r'.*[Нн]едостаточно средств.*|.*сумма на блокировку превышает.*?остаток на счете.*',
                                   additional_information['status_description']):
                continue

            error_count = next(error_counter)

            error_line = [request_info['registration_number'],
                          request_info['request_end_give_datetime'],
                          request_info['r_id'],
                          request_info['inn'],
                          request_info['request_status_id'],
                          additional_information['status'],
                          additional_information['status_description']]

            error_line = [str(line) for line in error_line]

            error_data.append(error_line)

        if error_count:
            print(error_text % error_count)
            for info_line in error_data:
                print(translit(str(namespace.data_separator.join(info_line)), 'ru', reversed=True))

            if error_count >= namespace.critical_limit:
                s_exit(CRITICAL)

            elif error_count >= namespace.warning_limit:
                s_exit(WARNING)
        else:
            print(no_error_text)
            s_exit(OK)

    except Exception as err:
        print('Plugin error')
        print(err)
        s_exit(UNKNOWN)

    show_version()
    print('For more information run use --help')
    s_exit(UNKNOWN)
