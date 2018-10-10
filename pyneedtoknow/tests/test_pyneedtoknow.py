
import json
from sys import argv
import unittest

from ..client import PgNeedToKnowClient

TABLES = {
    't1': {
        'table_name': 't1',
        'columns': [
            {'name': 'name', 'type': 'text'},
            {'name': 'age', 'type': 'int'},
            {'name': 'email', 'type': 'text'},
            {'name': 'country', 'type': 'text'}
        ],
        'description': 'personal and contact information'},
    't2': {
        'table_name': 't2',
        'columns': [
            {'name': 'votes', 'type': 'text'},
            {'name': 'orientation', 'type': 'text'},
            {'name': 'outlook', 'type': 'text'},
        ],
        'description': 'political beliefs'
    },
    't3': {
        'table_name': 't3',
        'columns': [
            {'name': 'ideological_view', 'type': 'text'},
            {'name': 'inflation_expectation', 'type': 'int'},
        ],
        'description': 'economic beliefs'
    },
    't4': {
        'table_name': 't4',
        'columns': [
            {'name': 'has_chronic_disease', 'type': 'text'},
            {'name': 'has_allergy', 'type': 'text'},
        ],
        'description': 'medical condition'
    },
}


class TestNtkHttpApi(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        cls.ntkc = PgNeedToKnowClient()
        cls.OWNERS = ['A', 'B', 'E', 'F']
        cls.OWNERS_METADATA = {'A': {'country': 'SE'}, 'B': {'country': 'SE'},
                               'E': {'country': 'NO'}, 'F': {'country': 'NO'}}
        cls.USERS = ['X', 'Y', 'Z']
        cls.USERS_METADATA = {'X': {'country': 'SE'}, 'Y': {'country': 'SE'},
                              'Z': {'country': 'NO'}}


    @classmethod
    def tearDownClass(cls):
        # clean up all DB state
        pass

    # Scalability scenario
    # eventually into own class

    def register_many(self, n, user_type):
        for i in range(n):
            self.ntkc.user_register({'user_id': str(i), 'user_type': user_type, 'user_metadata': {}})


    def delete_many(self, n, user_type, token):
        for i in range(n):
            self.ntkc.user_delete({'user_id': str(i), 'user_type': user_type}, token)


    def test_A_user_register(self):
        for owner in self.OWNERS:
            owner_data = {'user_id': owner, 'user_type': 'data_owner', 'user_metadata': self.OWNERS_METADATA[owner] }
            resp = self.ntkc.user_register(owner_data)
            self.assertEqual(resp.status_code, 200)
        for user in self.USERS:
            user_data = {'user_id': user, 'user_type': 'data_user', 'user_metadata': self.USERS_METADATA[user] }
            resp = self.ntkc.user_register(user_data)
            self.assertEqual(resp.status_code, 200)


    def test_B_table_create(self):
        token = self.ntkc.token(token_type='admin')
        for k in TABLES.keys():
            resp = self.ntkc.table_create({'definition': TABLES[k], 'type': 'mac'}, token)
            self.assertEqual(resp.status_code, 200)


    def test_C_table_describe(self):
        token = self.ntkc.token(token_type='admin')
        resp = self.ntkc.table_describe({'table_name': 't1', 'table_description': 'my description'}, token)
        self.assertEqual(resp.status_code, 200)


    def test_D_table_describe_columns(self):
        token = self.ntkc.token(token_type='admin')
        descriptions = {'table_name': 't1', 'column_descriptions': [
                       {'name': 'name', 'description': 'First and last name'},
                       {'name': 'age', 'description': 'Age in years'}]}
        resp = self.ntkc.table_describe_columns(descriptions, token)
        self.assertEqual(resp.status_code, 200)


    def test_E_table_metadata(self):
        token = self.ntkc.token(token_type='admin')
        resp = self.ntkc.table_metadata({'table_name': 't1'}, token)
        data = json.loads(resp.text)
        for coldata in data:
            if coldata['column_name'] == 'age':
                self.assertEqual(coldata['column_description'], 'Age in years')


    def _insert_test_data(self):
        owner_token_A = self.ntkc.token(user_id='A', token_type='owner')
        owner_token_B = self.ntkc.token(user_id='B', token_type='owner')
        owner_token_E = self.ntkc.token(user_id='E', token_type='owner')
        owner_token_F = self.ntkc.token(user_id='F', token_type='owner')
        self.ntkc.post_data({'name': 'A', 'age': 75, 'email': 'a@b.se', 'country': 'Sweden'}, owner_token_A, '/t1')
        self.ntkc.post_data({'name': 'B', 'age': 35, 'email': 'b@b.se', 'country': 'Sweden'}, owner_token_B, '/t1')
        self.ntkc.post_data({'name': 'E', 'age': 10, 'email': 'e@b.no', 'country': 'Norway'}, owner_token_E, '/t1')
        self.ntkc.post_data({'name': 'F', 'age': 18, 'email': 'f@b.no', 'country': 'Norway'}, owner_token_F, '/t1')
        self.ntkc.post_data({'has_chronic_disease': 'yes', 'has_allergy': 'yes'}, owner_token_A, '/t4')
        self.ntkc.post_data({'has_chronic_disease': 'no', 'has_allergy': 'yes'}, owner_token_B, '/t4')
        self.ntkc.post_data({'has_chronic_disease': 'yes', 'has_allergy': 'no'}, owner_token_E, '/t4')
        self.ntkc.post_data({'has_chronic_disease': 'yes', 'has_allergy': 'no'}, owner_token_F, '/t4')


    def _delete_test_data(self):
        owner_token_A = self.ntkc.token(user_id='A', token_type='owner')
        owner_token_B = self.ntkc.token(user_id='B', token_type='owner')
        owner_token_E = self.ntkc.token(user_id='E', token_type='owner')
        owner_token_F = self.ntkc.token(user_id='F', token_type='owner')
        for token in [owner_token_A, owner_token_B, owner_token_E, owner_token_F]:
            self.ntkc.user_delete_data({}, token)


    def test_F_default_data_access_policies(self):
        # 1. that data owners can only see their own data
        self._insert_test_data()
        owner_token_A = self.ntkc.token(user_id='A', token_type='owner')
        resp1 = self.ntkc.get_data(owner_token_A, '/t1')
        data = json.loads(resp1.text)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['row_owner'], 'owner_A')
        # 2. that data users do not have access by default
        user_token_X = self.ntkc.token(user_id='X', token_type='user')
        resp2 = self.ntkc.get_data(user_token_X, '/t1')
        self.assertEqual(resp2.status_code, 403)
        # 3. that admins cannot access data
        admin_token = self.ntkc.token(token_type='admin')
        resp3 = self.ntkc.get_data(admin_token, '/t1')
        self.assertEqual(len(json.loads(resp3.text)), 0)
        self._delete_test_data()


    def test_G_group_create(self):
        token = self.ntkc.token(token_type='admin')
        resp1 = self.ntkc.group_create({'group_name': 'group1', 'group_metadata': {}}, token)
        self.assertEqual(resp1.status_code, 200)
        resp2 = self.ntkc.group_create({'group_name': 'group2', 'group_metadata': {}}, token)
        self.assertEqual(resp2.status_code, 200)


    def test_H_group_add_and_remove_members(self):
        token = self.ntkc.token(token_type='admin')
        # 1. using named user IDs
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)
        named_members = {'group_name': 'group1', 'members': {'memberships': {'data_owners': ['A'], 'data_users': ['X']}}}
        resp2 = self.ntkc.group_add_members(named_members, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 2)
        resp3 = self.ntkc.group_remove_members(named_members, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)
        # 2. using metadata
        with_metadata = {'group_name': 'group1', 'metadata': {'key': 'country', 'value': 'SE'}}
        resp4 = self.ntkc.group_add_members(with_metadata, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 4)
        resp4 = self.ntkc.group_remove_members(with_metadata, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)
        # 3. add and removing all
        resp5 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 7)
        resp6 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)
        # 4. adding all data owners
        resp7 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all_owners': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 4)
        resp8 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)
        # 5. adding all data users
        resp9 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all_users': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 3)
        resp10 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, token)
        resp1 = self.ntkc.group_list_members({'group_name': 'group1'}, token)
        self.assertEqual(len(json.loads(resp1.text)), 0)


    def test_I_table_group_access_grant_and_revoke(self):
        admin_token = self.ntkc.token(token_type='admin')
        self._insert_test_data()
        resp1 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all': True}, admin_token)
        # 1. test that no access
        user_token_X = self.ntkc.token(user_id='X', token_type='user')
        resp2 = self.ntkc.get_data(user_token_X, '/t1')
        self.assertEqual(resp2.status_code, 403)
        # 2. grant table
        grant_info = {'table_name': 't1', 'group_name': 'group1', 'grant_type': 'select'}
        resp3 = self.ntkc.table_group_access_grant(grant_info, admin_token)
        # 3. test access works
        resp4 = self.ntkc.get_data(user_token_X, '/t1')
        self.assertEqual(resp4.status_code, 200)
        self.assertEqual(len(json.loads(resp4.text)), 4)
        # 4. remove grant
        resp5 = self.ntkc.table_group_access_revoke(grant_info, admin_token)
        # 5. test that no access
        resp6 = self.ntkc.get_data(user_token_X, '/t1')
        self.assertEqual(resp6.status_code, 403)
        resp7 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, admin_token)
        self._delete_test_data()


    def test_J_user_groups(self):
        admin_token = self.ntkc.token(token_type='admin')
        owner_token_A = self.ntkc.token(user_id='A', token_type='owner')
        resp1 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all': True}, admin_token)
        resp2 = self.ntkc.user_groups({'user_id': 'A', 'user_type': 'data_owner'}, admin_token)
        self.assertEqual(json.loads(resp2.text)[0]['group_name'], 'group1')
        resp3 = self.ntkc.user_groups({'user_type': 'data_owner'}, owner_token_A)
        self.assertEqual(json.loads(resp3.text)[0]['group_name'], 'group1')
        resp4 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, admin_token)


    def test_K_user_group_remove(self):
        admin_token = self.ntkc.token(token_type='admin')
        owner_token_A = self.ntkc.token(user_id='A', token_type='owner')
        resp1 = self.ntkc.group_add_members({'group_name': 'group1', 'add_all': True}, admin_token)
        resp2 = self.ntkc.user_group_remove({'group_name': 'group1'}, owner_token_A)
        resp3 = self.ntkc.user_groups({'user_type': 'data_owner'}, owner_token_A)
        self.assertEqual(len(json.loads(resp3.text)), 0)
        resp4 = self.ntkc.group_remove_members({'group_name': 'group1', 'remove_all': True}, admin_token)


    def test_L_table_overview(self):
        admin_token = self.ntkc.token(token_type='admin')
        resp = self.ntkc.get_table_overview(admin_token)
        overview = json.loads(resp.text)
        tables = []
        for i in overview:
            tables.append(i['table_name'])
        for j in TABLES.keys():
            self.assertTrue(j in tables)


    def test_M_get_user_registrations(self):
        admin_token = self.ntkc.token(token_type='admin')
        resp  = self.ntkc.get_user_registrations(admin_token)
        users = json.loads(resp.text)
        ids = []
        for i in users:
            ids.append(i['user_id'])
        defined_users = []
        defined_users.extend(self.OWNERS)
        defined_users.extend(self.USERS)
        for j in defined_users:
            self.assertTrue(j in ids)

    # groups
    # event_log*

    def test_Y_group_delete(self):
        token = self.ntkc.token(token_type='admin')
        resp1 = self.ntkc.group_delete({'group_name': 'group1'}, token)
        self.assertEqual(resp1.status_code, 200)
        resp2 = self.ntkc.group_delete({'group_name': 'group2'}, token)
        self.assertEqual(resp2.status_code, 200)


    def test_Z_user_delete(self):
        token = self.ntkc.token(token_type='admin')
        for owner in self.OWNERS:
            resp = self.ntkc.user_delete({'user_id': owner, 'user_type': 'data_owner'}, token)
            self.assertEqual(resp.status_code, 200)
        for user in self.USERS:
            resp = self.ntkc.user_delete({'user_id': user, 'user_type': 'data_user'}, token)
            self.assertEqual(resp.status_code, 200)


    def test_ZA_create_many(self):
        self.register_many(10000, 'data_owner')
        self.register_many(100, 'data_user')


    def test_ZB_delete_many(self):
        token = self.ntkc.token(token_type='admin')
        self.delete_many(10000, 'data_owner', token)
        self.delete_many(100, 'data_user', token)


def main():
    if len(argv) < 2:
        print 'not enough arguments'
        print 'need either "--correctness" or "--scalability"'
        return
    runner = unittest.TextTestRunner()
    suite = []
    correctness_tests = [
        'test_A_user_register',
        'test_B_table_create',
        'test_C_table_describe',
        'test_D_table_describe_columns',
        'test_E_table_metadata',
        'test_F_default_data_access_policies',
        'test_G_group_create',
        'test_H_group_add_and_remove_members',
        'test_I_table_group_access_grant_and_revoke',
        'test_J_user_groups',
        'test_K_user_group_remove',
        'test_L_table_overview',
        'test_M_get_user_registrations',
        'test_Y_group_delete',
        'test_Z_user_delete',
    ]
    scalability_tests = [
        'test_ZA_create_many',
        'test_ZB_delete_many',
    ]
    correctness_tests.sort()
    if argv[1] == '--correctness':
        suite.append(unittest.TestSuite(map(TestNtkHttpApi, correctness_tests)))
    elif argv[1] == '--scalability':
        suite.append(unittest.TestSuite(map(TestNtkHttpApi, scalability_tests)))
    else:
        print "unrecognised argument"
        print 'need either "--correctness" or "--scalability"'
        return
    map(runner.run, suite)
    return


if __name__ == '__main__':
    main()
