class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self._executed = None
        self._result = None
        self._rows = None

    def execute(self, query):
        result = self._query(query)
        self._executed = query
        return result

    def _query(self, query):
        self._last_executed = query
        self._reset_result()
        self.connection.query(query)
        self._format_result()
        return self.rowcount

    def _format_result(self):
        pass

    def _reset_result(self):
        self.rownumber = 0
        self._result = None
        self.rowcount = 0
        self.description = None
        self.lastrowid = None
        self._rows = None
