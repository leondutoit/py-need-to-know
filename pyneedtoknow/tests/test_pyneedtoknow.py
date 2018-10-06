
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
        owner_data = {'user_id': '1', 'user_type': 'data_owner', 'user_metadata': {}}
        resp1 = self.ntkc.user_register(owner_data)
        self.assertEqual(resp1.status_code, 200)
        user_data = {'user_id': '1', 'user_type': 'data_user', 'user_metadata': {}}
        resp2 = self.ntkc.user_register(user_data)
        self.assertEqual(resp2.status_code, 200)


    def test_B_table_create(self):
        token = self.ntkc.token(token_type='admin')
        for k in TABLES.keys():
            resp = self.ntkc.table_create({'definition': TABLES[k], 'type': 'mac'}, token)
            self.assertEqual(resp.status_code, 200)


    def test_C_table_describe(self):
        token = self.ntkc.token(token_type='admin')
        resp = self.ntkc.table_describe({'table_name': 't1', 'table_description': 'my description'}, token)
        self.assertEqual(resp.status_code, 200)


    def test_Z_user_delete(self):
        token = self.ntkc.token(token_type='admin')
        resp1 = self.ntkc.user_delete({'user_id': '1', 'user_type': 'data_owner'}, token)
        self.assertEqual(resp1.status_code, 200)
        resp2 = self.ntkc.user_delete({'user_id': '1', 'user_type': 'data_user'}, token)
        self.assertEqual(resp2.status_code, 200)


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
