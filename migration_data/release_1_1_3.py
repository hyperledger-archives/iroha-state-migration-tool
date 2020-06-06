def do_nothing(cursor, block_storage):
    pass


TRANSITIONS = (
    {
        'from': ('1.1.2'),
        'to': ('1.1.3'),
        'function': do_nothing
    },
    {
        'from': ('1.1.3'),
        'to': ('1.1.2'),
        'function': do_nothing
    },
)
