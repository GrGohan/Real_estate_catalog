import sys

from psycopg2 import OperationalError
import psycopg2


def print_psycopg2_exception(err):
    """ Function for handling postgres exceptions """

    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno

    print("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)

    print("\nextensions.Diagnostics:", err.diag)

    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def psycopg_connection():
    """ Get connection to database """

    try:
        connection = psycopg2.connect(
            database="real_estate",
            user="postgres",
            password="postgres",
            host="127.0.0.1",
            port="5432"
        )
    except OperationalError as err:
        print_psycopg2_exception(err)
        connection = None

    return connection


def get_list_of_real_estate(filter):
    """ Get fields from table real_estate_objects """

    connection = psycopg_connection()

    if connection is not None:
        cursor = connection.cursor()
        try:


            query = "SELECT objs_rea.id, objs_rea.title, objs_rea.floor, objs_rea.square, m.title"\
                    " FROM real_estate_objects as objs_rea"\
                    " left join reest_metro as rm on objs_rea.id = rm.id_re"\
                    " left join metro as m on rm.id_metro = m.id"

            print(filter)
            args = []
            condition = []

            if "metro" in filter:
                metro = filter["metro"]
                condition.append("rm.id_metro is NULL" if metro == 0 else "rm.id_metro = %s")
                args.append(metro)

            if "square_min" in filter and "square_max" in filter and int(filter["square_min"]) < int(filter["square_max"]):
                condition.append("square BETWEEN %s AND %s")
                args.append(filter["square_min"])
                args.append(filter["square_max"])

            if "floor_min" in filter and "floor_max" in filter and int(filter["floor_min"]) < int(filter["floor_max"]):
                condition.append("floor BETWEEN %s AND %s")
                args.append(filter["floor_min"])
                args.append(filter["floor_max"])

            if len(condition) != 0:
                query += " WHERE " + " AND ".join(condition)

            query += " ORDER BY objs_rea.id"
            if len(args) == 0:
                cursor.execute(query)
            else:
                cursor.execute(query, args)

        except Exception as err:
            print_psycopg2_exception(err)
            connection.rollback()

        real_estate = cursor.fetchall()

        connection.close()

        res = {}
        for model in real_estate:
            id = model[0]

            if id not in res:
                res[id] = {
                    "title": model[1],
                    "floor": model[2],
                    "square": model[3],
                    "metro": [model[4],] if model[4] else []
                }
            else:
                res[id]['metro'].append(model[4])

        return res


def get_detail_of_real_estate(id):
    """ Get detail info from table real_estate_objects """

    connection = psycopg_connection()

    if connection is not None:
        cursor = connection.cursor()
        cursor_metro = connection.cursor()
        try:
            cursor.execute("SELECT title, address, floor, square, type"
                           " FROM real_estate_objects"
                           " WHERE id = %s", (id,))
            cursor_metro.execute("SELECT metro.title"
                                 " FROM reest_metro as objs_metro inner join metro on objs_metro.id_metro = metro.id"
                                 " WHERE objs_metro.id_re = %s", (id,))
        except Exception as err:
            print_psycopg2_exception(err)
            connection.rollback()

        real_estate = cursor.fetchall()[0]
        metro = cursor_metro.fetchall()
        connection.close()

        return {
            "title": real_estate[0],
            "address": real_estate[1],
            "floor": real_estate[2],
            "square": real_estate[3],
            "type": real_estate[4],
            "metro": [x[0] for x in metro]
        }


def get_list_of_metro():
    """ Get list of stations from table metro """

    connection = psycopg_connection()

    if connection is not None:
        cursor = connection.cursor()
        try:
            query = "SELECT id, title FROM metro"
            cursor.execute(query)
        except Exception as err:
            print_psycopg2_exception(err)
            connection.rollback()

        real_estate = cursor.fetchall()
        connection.close()

        res = []
        for model in real_estate:
            res.append({
                "id": model[0],
                "title": model[1],
            })

        return res

