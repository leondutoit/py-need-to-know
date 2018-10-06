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

# assuming the admin has authenticated themselves elsewhere
admin_token = c.token(token_type='admin')

# create basic DB objects
for table in TABLES.keys()
    c.table_create(TABLES[table], admin_token)
for owner in DATA_OWNERS:
    owner_data = {'user_id': owner, 'user_type': 'data_owner',
                  'user_metadata': {}}
    c.user_register(owner_data)
for user in DATA_USERS:
    user_data = {'user_id': user, 'user_type': 'data_user',
                 'user_metadata': {}}
    c.user_register(user_data)

# collect data
for owner in DATA_OWNERS:
    # assuming users have authenticated themselves elsewhere
    owner_token = c.token(user_id=owner, token_type='owner')
    # in reality the data would be different for each owner :)
    c.post_data({'name': 'some name', 'age': 19,
                 'email': 'my@email.com', 'country': 'Norway'},
                token, '/t1')
    c.post_data({'has_chronic_disease': 'yes', 'has_allergy': 'no'},
                token, '/t2')

# create groups
c.group_create({'group_name': 'group1',
                'group_metadata': {'explanation': 'limited access'}},
                admin_token)
c.group_create({'group_name': 'group2',
                'group_metadata': {'explanation': 'full access'}},
                admin_token)

# add members
# group1
c.group_add_members({'group_name': 'group1',
                     'memberships': {
                        'data_users': ['X', 'Y'],
                        'data_owners': ['A', 'B', 'C', 'D']}},
                    admin_token)
# group2
c.group_add_members({'group_name': 'group2',
                     'memberships': {
                        'data_users': ['Z']}},
                    admin_token)
c.group_add_members({'group_name': 'group2',
                     'all_owners': True},
                     admin_token)

# add table grants
c.table_group_access_grant({'table_name': 't1', 'group_name': 'group1',
                            'grant_type': 'select'}, admin_token)
c.table_group_access_grant({'table_name': 't1', 'group_name': 'group2',
                            'grant_type': 'select'}, admin_token)
c.table_group_access_grant({'table_name': 't2', 'group_name': 'group2',
                            'grant_type': 'select'}, admin_token)
```
