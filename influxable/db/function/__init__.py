def _generate_function(identifier):
    class Function:
        def __init__(self, field='*'):
            self.field = field
            self.func_alias = None

        def evaluate(self):
            if hasattr(self.field, 'evaluate'):
                field = self.field.evaluate()
            else:
                field = self.field
            return '{}({})'.format(identifier, field) if not self.func_alias  \
                else '{}({}) as {}'.format(identifier, field, self.func_alias)

        def alias(self, alias):
            if not isinstance(alias, str):
                return self  # TODO: raise error
            self.func_alias = alias
            return self

    return Function


def _generate_function_with_param(identifier):
    class Function:
        def __init__(self, n, *fields):
            self.n = n
            self.fields = fields
            self.func_alias = ''

        def evaluate(self):
            fields = ', '.join(self.fields)
            return '{}({}, {})'.format(identifier, fields, self.n) if not self.func_alias \
                else '{}({}, {}) as {}'.format(identifier, fields, self.n, self.func_alias)

        def alias(self, alias):
            if not isinstance(alias, str):
                return self  # TODO: raise error
            self.func_alias = alias
            return self

    return Function
