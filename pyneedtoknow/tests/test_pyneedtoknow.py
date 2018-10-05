
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
    't2': {},
    't3': {},
    't4': {},
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

    def register_many(self, n, owner=False, user=False):
        for i in range(n):
            if owner:
                user_type = 'data_owner'
            elif user:
                user_type = 'data_user'
            self.ntkc.call(method='user_register',
                           data={'user_id': str(i), 'user_type': user_type, 'user_metadata': {}},
                           user_type='anon')


    def delete_many(self, n, owner=False, user=False):
        for i in range(n):
            user_id = str(i)
            if owner:
                user_type = 'data_owner'
            elif user:
                user_type = 'data_user'
            self.ntkc.call(method='user_delete',
                           data={'user_id': user_id, 'user_type': user_type},
                           user_type='admin')


    def test_A_user_register(self):
        owner_data = {'user_id': '1', 'user_type': 'data_owner', 'user_metadata': {}}
        resp1 = self.ntkc.call(method='user_register',
                               data=owner_data,
                               user_type='anon')
        self.assertEqual(resp1.status_code, 200)
        user_data = {'user_id': '1', 'user_type': 'data_user', 'user_metadata': {}}
        resp2 = self.ntkc.call(method='user_register',
                               data=user_data,
                               user_type='anon')
        self.assertEqual(resp2.status_code, 200)


    def test_B_table_create(self):
        resp1 = self.ntkc.call(method='table_create',
                               data={'definition': TABLES['t1'], 'type': 'mac'},
                               user_type='admin')
        print resp1.text
        self.assertEqual(resp1.status_code, 200)

    def test_Z_user_delete(self):
        resp1 = self.ntkc.call(method='user_delete',
                               data={'user_id': '1', 'user_type': 'data_owner'},
                               user_type='admin')
        self.assertEqual(resp1.status_code, 200)
        resp2 = self.ntkc.call(method='user_delete',
                               data={'user_id': '1', 'user_type': 'data_user'},
                               user_type='admin')
        self.assertEqual(resp2.status_code, 200)


    def test_ZA_create_many(self):
        self.register_many(10000, owner=True)


    def test_ZB_delete_many(self):
        self.delete_many(10000, owner=True)


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
        'test_Z_user_delete'
    ]
    scalability_tests = [
        'test_ZA_create_many',
        'test_ZB_delete_many'
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
