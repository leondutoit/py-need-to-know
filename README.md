# py-need-to-know

python client for [pg-need-to-know](https://github.com/leondutoit/pg-need-to-know).

## Key terms

- Data owner: a person from whom data originates
- Data user: a person analysing the data
- Administrator: a person who manages access control, and is responsible for the ethical use of data

## Usage

Assume the following setup:

```txt
data owners: A, B, C, D, E, F
tables: t1, t2, containing data from all data owners
data users: X, Y, Z
```

Now suppose we need to set up the following access control rules in our DB:

```txt
data users X, and Y should only have access to data in tables t1 and only data from owners A, B, C, D
data user Z should have access to all data - i.e. tables t1, t2
```

That means we need the following groups, and table grants:

```txt
group1
    - members: ((X, Y), (A, B, C, D))
    - select table access grant: (t1)
group2
    - members: ((Z), (A, B, C, D, E, F))
    - select table access grants: (t1, t2)
```

Here is how to use `py-need-to-know` to accomplish this.

```python
from pyneedtoknow import client

c = client.PgNeedToKnowClient()

TABLES = {
    't1' = {
        'table_name': 't1',
        'columns': [
            {'name': 'name', 'type': 'text'},
            {'name': 'age', 'type': 'int'},
            {'name': 'email', 'type': 'text'},
            {'name': 'country', 'type': 'text'}
        ],
        'description': 'personal and contact information'
    }
    't2': {
        'table_name': 't2',
        'columns': [
            {'name': 'has_chronic_disease', 'type': 'text'},
            {'name': 'has_allergy', 'type': 'text'},
        ],
        'description': 'medical condition'
    },
}
DATA_OWNERS = ['A', 'B', 'C', 'D', 'E', 'F']
DATA_USERS = ['X', 'Y', 'Z']

token = c.token(token_type='admin')

# create basic DB objects
for table in TABLES.keys()
    c.table_create(TABLES[table], token)
for owner in DATA_OWNERS:
    owner_data = {'user_id': owner, 'user_type': 'data_owner',
                  'user_metadata': {}}
    c.user_register(owner_data, token)
for user in DATA_USERS:
    user_data = {'user_id': user, 'user_type': 'data_user',
                 'user_metadata': {}}
    c.user_register(user_data, token)

# collect data
```
