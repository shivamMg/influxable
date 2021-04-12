# copied from:
# https://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons


class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.
    The decorated class can define one `__init__` function that
    takes only the `self` argument. Also, the decorated class cannot be
    inherited from. Other than that, there are no restrictions that apply
    to the decorated class.
    To get the singleton instance, use the `instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.
    """

    def __init__(self, decorated):
        self.db_name_map = {}
        self._decorated = decorated

    def get_instance(self, *args, **kwargs):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.
        """
        db_name = kwargs.get('database_name')
        if db_name in self.db_name_map:
            return self.db_name_map[db_name]
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self, *args, **kwargs):
        db_name = kwargs.get('database_name', 'default')
        if db_name and db_name not in self.db_name_map:
            self.db_name_map[db_name] = self._decorated(*args, **kwargs)

        if not hasattr(self, '_instance'):
            self._instance = self.db_name_map[db_name]
        return self.db_name_map[db_name]

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)
