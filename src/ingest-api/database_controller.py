import psycopg2


class MyDatabase:
    def __init__(self, arg_db="linkedin_scrapper", arg_user="postgres"):
        self.db_name = arg_db
        self.conn = psycopg2.connect(
            host="localhost",
            database=arg_db,
            user=arg_user,
            password="example")
        self.cur = self.conn.cursor()

    def run_select_sql(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

    def run_DDL_sql(self, query):
        self.cur.execute(query)

    def commit_sql(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':

    db = MyDatabase()
    result = db.run_select_sql("SELECT * FROM leads LIMIT 10;")
    print(result)
    db.commit_sql()
    db.close()
