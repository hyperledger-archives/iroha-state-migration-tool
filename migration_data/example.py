def migrate1(cursor):
    cursor.execute('create table hey_this_is_new_in_1_1_0 ()')


def migrate2(cursor):
    cursor.execute('create table hey_this_is_new_in_1_1_1 ()')


def migrate3(cursor):
    cursor.execute('drop table hey_this_is_new_in_1_1_1')
    cursor.execute('drop table hey_this_is_new_in_1_1_0')


TRANSITIONS = (
    {'from': (1, 0, 0), 'to': (1, 1, 0), 'function': migrate1},
    {'from': (1, 1, 0), 'to': (1, 1, 1), 'function': migrate2},
    {'from': (1, 1, 1), 'to': (1, 0, 0), 'function': migrate3},
)
