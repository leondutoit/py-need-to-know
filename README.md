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
data users X, and Y should only have access to data contained in tables t1
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

table_definition = {
    'table_name': 't1',
    'columns': [
        {'name': 'name', 'type': 'text'},
        {'name': 'age', 'type': 'int'},
        {'name': 'email', 'type': 'text'},
        {'name': 'country', 'type': 'text'}
    ],
    'description': 'personal and contact information'
}

# get an admin token
# from the API, or some other issuer
token = c.token(token_type='admin')
c.table_create(table_definition, token)

# and so on...
```
