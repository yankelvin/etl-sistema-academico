import pyodbc


class Repository:
    def __init__(self):
        self.conn = None

    def Open(self):
        self.conn = pyodbc.connect('Driver={SQL Server};Server=NT-5\SQLEXPRESS;Database=Academico;'
                                   'Trusted_Connection=yes;MultipleActiveResultSets=true')
        return self.conn

    def Close(self):
        self.conn.close()

    def ExecuteCommand(self, command):
        cursor = self.conn.cursor()
        cursor.execute(command)

        self.conn.commit()

    def ExecuteQuery(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)

        return cursor
