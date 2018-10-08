
import json

import requests

class PgNeedToKnowClient(object):

    """
    API client for pg-need-to-know as exposed via postgrest's HTTP interface.
    """

    def __init__(self, url=None, api_endpoints=None):
        if not url:
            self.url = 'http://localhost:3000'
        else:
            self.url = url
        if not api_endpoints:
            self.api_endpoints = {
                'table_create': '/rpc/table_create',
                'table_describe': '/rpc/table_describe',
                'table_describe_columns': '/rpc/table_describe_columns',
                'user_register': '/rpc/user_register',
                'user_delete': '/rpc/user_delete',
            }
        else:
            self.api_endpoints = api_endpoints
        self.funcs = {
            'table_create': self.table_create,
            'table_describe': self.table_describe,
            'table_describe_columns': self.table_describe_columns,
            'user_register': self.user_register,
            'user_delete': self.user_delete,

        }

    def call(self, method=None, data=None, identity=None, user_type=None):
        if user_type == 'anon':
            token = None
        elif user_type == 'admin':
            token = self.token(token_type='admin')
        elif user_type == 'owner':
            token = self.token(user_id=identity, token_type='owner')
        elif user_type == 'user':
            token = self.token(user_id=identity, token_type='user')
        else:
            raise Exception('Unknown user type, cannot proceed')
        try:
            func = self.funcs[method]
            endpoint = self.api_endpoints[method]
        except KeyError:
            raise Exception('Cannot look up client function based on provided method')
        return func(data, token, endpoint)

    # helper functions

    def _assert_keys_present(self, required_keys, existing_keys):
        try:
            for rk in required_keys:
                assert rk in existing_keys
        except AssertionError:
            raise Exception('Missing required key in data')


    def _http_get(self, endpoint, headers=None):
        url = self.url + endpoint
        if not headers:
            headers = None
        return requests.get(url, headers=headers)


    def _http_post_unauthenticated(self, endpoint, payload=None):
        return self._http_post(endpoint, {'Content-Type': 'application/json'},
                               payload)


    def _http_post_authenticated(self, endpoint, payload=None, token=None):
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        return self._http_post(endpoint, headers, payload)


    def _http_post(self, endpoint, headers, payload=None):
        url = self.url + endpoint
        if payload:
            return requests.post(url, headers=headers, data=json.dumps(payload))
        else:
            return requests.post(url, headers=headers)


    def token(self, user_id=None, token_type=None):
        if user_id:
            endpoint = '/rpc/token?user_id=' + user_id + '&token_type=' + token_type
        else:
            endpoint = '/rpc/token?token_type=' + token_type
        resp = self._http_get(endpoint)
        return json.loads(resp.text)['token']


    # table functions

    def table_create(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'definition': {
                'table_name': 't1',
                'columns': [
                    {'name': 'c1', 'type': 'text', 'description': 'some column'},]
                }
            }
            Note: all PostgreSQL data types are allowed.
        token: str
            JWT, role=admin
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['table_create']
        self._assert_keys_present(['definition', 'type'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def table_describe(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'table_name': 't1',
             'table_description': 'Personal and contact information'}
        token: str
            JWT, role=admin
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['table_describe']
        self._assert_keys_present(['table_name', 'table_description'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def table_describe_columns(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'table_name': 't1',
             'column_descriptions': [
                    {'name': 'c1', 'description': 'my column'}
                ]
            }
        token: str
            JWT
        endpoint: str
        """
        if not endpoint:
            endpoint = self.api_endpoints['table_describe_columns']
        self._assert_keys_present(['table_name', 'column_descriptions'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def table_metadata(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def table_group_access_grant(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def table_group_access_revoke(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass

    # user functions

    def user_register(self, data, token=None, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'user_id': '12345',
             'user_type': <data_owner, data_user>,
             'user_metadata': {'institution': 'A'}}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_register']
        self._assert_keys_present(['user_id', 'user_type', 'user_metadata'], data.keys())
        assert data['user_type'] in ['data_owner', 'data_user']
        return self._http_post_unauthenticated(endpoint, payload=data)


    def user_group_remove(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def user_groups(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def user_delete_data(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def user_delete(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'user_id': '12345',
             'user_type': <data_owner, data_user>}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_delete']
        self._assert_keys_present(['user_id', 'user_type'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)

    # group functions

    def group_create(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def group_add_members(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def _group_add_members_members(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def _group_add_members_metadata(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def _group_add_members_all_owners(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def _group_add_members_all_users(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def _group_add_members_all(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def group_list_members(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def group_remove_members(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def group_delete(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass

    # informational views, tables, and event logs

    def get_table_overview(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_user_registrations(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_groups(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_event_log_user_group_removals(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_event_log_user_data_deletions(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_event_log_data_access(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass


    def get_event_log_access_control(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str
        """
        pass

    # utility functions (not in the SQL API)

    def post_data(self, data, token, endpoint):
        pass


    def patch_data(self, data, token , endpoint):
        pass


    def get_data(self, token, endpoint):
        pass
