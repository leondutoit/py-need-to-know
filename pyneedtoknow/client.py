
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
                'table_metadata': '/rpc/table_metadata',
                'group_create': '/rpc/group_create',
                'group_add_members': '/rpc/group_add_members',
                'group_remove_members': '/rpc/group_remove_members',
                'group_delete': '/rpc/group_delete',
                'user_register': '/rpc/user_register',
                'user_delete_data': '/rpc/user_delete_data',
                'user_delete': '/rpc/user_delete',
            }
        else:
            self.api_endpoints = api_endpoints
        self.funcs = {
            'table_create': self.table_create,
            'table_describe': self.table_describe,
            'table_describe_columns': self.table_describe_columns,
            'table_metadata': self.table_metadata,
            'group_create': self.group_create,
            'group_add_members': self.group_add_members,
            'group_remove_members': self.group_remove_members,
            'group_delete': self.group_delete,
            'user_register': self.user_register,
            'user_delete_data': self.user_delete_data,
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


    def _http_patch_authenticated(self, endpoint, payload=None, token=None):
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        url = self.url + endpoint
        return requests.patch(url, headers=headers, data=json.dumps(payload))


    def token(self, user_id=None, token_type=None):
        if user_id:
            endpoint = '/rpc/token?user_id=' + user_id + '&token_type=' + token_type
        else:
            endpoint = '/rpc/token?token_type=' + token_type
        resp = self._http_get(endpoint)
        return json.loads(resp.text)['token']


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
            {'table_name': 't1'}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['table_metadata']
        endpoint += '?table_name=%s' % data['table_name']
        headers = {'Authorization': 'Bearer ' + token}
        return self._http_get(endpoint, headers)


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
        data: None
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_delete_data']
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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


    def group_create(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'group_name': 'group1', 'group_metadata': {'key': 'val'}}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_create']
        self._assert_keys_present(['group_name', 'group_metadata'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def group_add_members(self, data, token, endpoint=None):
        """
        Add members to a group.

        There are 5 methods of doing this:
        1. By providing specific user IDs
        2. By providing a key-value pair to choose IDs based on metadata
        3. Add all registered data owners and data users
        4. Add all registered data owners
        5. Add all registered data users

        Which one is used, depends on the structure of the data dict.

        Parameters
        ----------
        data: dict
            1. {'group_name': 'group1', 'memberships': {'data_owners': ['1', '2'], 'data_users': ['3', '4']}}, or
            2. {'group_name': 'group1', 'metadata': {'key': 'country', 'value': 'NO'}}, or
            3. {'group_name': 'group1', 'add_all': true}, or
            4. {'group_name': 'group1', 'add_all_owners': true}, or
            5. {'group_name': 'group1', 'add_all_users': true}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_add_members']
        keys = data.keys()
        if 'memberships' in keys:
            return self._group_add_members_members(data, token, endpoint)
        elif 'metadata' in keys:
            return self._group_add_members_metadata(data, token, endpoint)
        elif 'add_all' in keys:
            return self._group_add_members_all(data, token, endpoint)
        elif 'add_all_owners' in keys:
            return self._group_add_members_all_owners(data, token, endpoint)
        elif 'add_all_users' in keys:
            return self._group_add_members_all_users(data, token, endpoint)
        else:
            raise Exception('Could not match keys to a method')


    def _group_add_members_members(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'memberships'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_add_members_metadata(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'metadata'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_add_members_all_owners(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'add_all_owners'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_add_members_all_users(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'add_all_users'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_add_members_all(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'add_all'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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
        Remove members from a group.

        There are three ways of doing this:
        1. By providing specific user IDs
        2. By a key-value pair to choose IDs based on metadata
        3. Removing all current members

        Which one is used, depends on the structure of the data dict.

        Parameters
        ----------
        data: dict
            1. {'group_name': 'group1', 'memberships': {'data_owners': ['1', '2'], 'data_users': ['3', '4']}}, or
            2. {'group_name': 'group1', 'metadata': {'key': 'country', 'value': 'NO'}}, or
            3. {'group_name': 'group1', 'remove_all': true}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_remove_members']
        keys = data.keys()
        if 'memberships' in keys:
            return self._group_remove_members_members(data, token, endpoint)
        elif 'metadata' in keys:
            return self._group_remove_members_metadata(data, token, endpoint)
        elif 'remove_all' in keys:
            return self._group_remove_members_all(data, token, endpoint)
        else:
            raise Exception('Could not match keys to a method')



    def _group_remove_members_members(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'memberships'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_remove_members_metadata(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'metadata'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def _group_remove_members_all(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'remove_all'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def group_delete(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_delete']
        self._assert_keys_present(['group_name'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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


    def post_data(self, data, token, endpoint):
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def patch_data(self, data, token , endpoint):
        return self._http_patch_authenticated(endpoint, payload=data, token=token)


    def get_data(self, token, endpoint):
        headers = {'Authorization': 'Bearer ' + token}
        return self._http_get(endpoint, headers)
