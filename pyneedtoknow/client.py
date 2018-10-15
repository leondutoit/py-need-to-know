
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
            # over-ride this if your proxy has custom routing
            self.api_endpoints = {
                'table_create': '/rpc/table_create',
                'table_describe': '/rpc/table_describe',
                'table_describe_columns': '/rpc/table_describe_columns',
                'table_metadata': '/rpc/table_metadata',
                'table_group_access_grant': '/rpc/table_group_access_grant',
                'table_group_access_revoke': '/rpc/table_group_access_revoke',
                'group_create': '/rpc/group_create',
                'group_add_members': '/rpc/group_add_members',
                'group_list_members': '/rpc/group_list_members',
                'group_remove_members': '/rpc/group_remove_members',
                'group_delete': '/rpc/group_delete',
                'user_register': '/rpc/user_register',
                'user_group_remove': '/rpc/user_group_remove',
                'user_groups': '/rpc/user_groups',
                'user_delete_data': '/rpc/user_delete_data',
                'user_delete': '/rpc/user_delete',
                'table_overview': '/table_overview',
                'user_registrations': '/user_registrations',
                'groups': '/groups',
                'event_log_user_group_removals': '/event_log_user_group_removals',
                'event_log_user_data_deletions': '/event_log_user_data_deletions',
                'event_log_data_access': '/event_log_data_access',
                'event_log_access_control': '/event_log_access_control',
                'event_log_data_updates': '/event_log_data_updates',
            }
        else:
            self.api_endpoints = api_endpoints


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
        if not endpoint:
            endpoint = self.api_endpoints['table_group_access_grant']
        self._assert_keys_present(['table_name', 'group_name', 'grant_type'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def table_group_access_revoke(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['table_group_access_revoke']
        self._assert_keys_present(['table_name', 'group_name', 'grant_type'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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
            {'group_name': 'group1'}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_group_remove']
        self._assert_keys_present(['group_name'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def user_groups(self, data, token, endpoint=None):
        """
        Parameters
        ----------
        data: dict
            {'user_id': <id, optional key>, 'user_type': <data_owner,data_user>}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_groups']
        self._assert_keys_present(['user_type'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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
            1. {'group_name': 'group1', 'members': {'memberships': {'data_owners': ['1', '2'], 'data_users': ['3', '4']}}}, or
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
        if 'members' in keys:
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
        self._assert_keys_present(['group_name', 'members'], data.keys())
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
            {'group_name': 'group1'}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_list_members']
        self._assert_keys_present(['group_name'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)


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
            1. {'group_name': 'group1', 'members': {'memberships': {'data_owners': ['1', '2'], 'data_users': ['3', '4']}}}, or
            2. {'group_name': 'group1', 'metadata': {'key': 'country', 'value': 'NO'}}, or
            3. {'group_name': 'group1', 'remove_all': true}
        token: str
            JWT
        endpoint: str

        """
        if not endpoint:
            endpoint = self.api_endpoints['group_remove_members']
        keys = data.keys()
        if 'members' in keys:
            return self._group_remove_members_members(data, token, endpoint)
        elif 'metadata' in keys:
            return self._group_remove_members_metadata(data, token, endpoint)
        elif 'remove_all' in keys:
            return self._group_remove_members_all(data, token, endpoint)
        else:
            raise Exception('Could not match keys to a method')



    def _group_remove_members_members(self, data, token, endpoint):
        self._assert_keys_present(['group_name', 'members'], data.keys())
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


    def get_table_overview(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{table_name, table_description, groups_with_access}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['table_overview']
        return self.get_data(token, endpoint)


    def get_user_registrations(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{registration_date, user_id, user_name, user_type, user_metadata}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['user_registrations']
        return self.get_data(token, endpoint)


    def get_groups(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{group_name, group_metadata}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['groups']
        return self.get_data(token, endpoint)


    def get_event_log_user_group_removals(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{removal_date, user_name, group_name}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['event_log_user_group_removals']
        return self.get_data(token, endpoint)


    def get_event_log_user_data_deletions(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{user_name, request_date}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['event_log_user_data_deletions']
        return self.get_data(token, endpoint)


    def get_event_log_data_access(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{request_time, row_id, data_user, data_owner}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['event_log_data_access']
        return self.get_data(token, endpoint)


    def get_event_log_access_control(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{id, event_time, event_type, group_name, target}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['event_log_access_control']
        return self.get_data(token, endpoint)


    def get_event_log_data_updates(self, token, endpoint=None):
        """
        Parameters
        ----------
        token: str
            JWT
        endpoint: str

        Returns
        -------
        requests.Response

            [{updated_time, updated_by, table_name, row_id, column_name, old_data, new_data}]

        """
        if not endpoint:
            endpoint = self.api_endpoints['event_log_data_updates']
        return self.get_data(token, endpoint)


    def post_data(self, data, token, endpoint):
        return self._http_post_authenticated(endpoint, payload=data, token=token)


    def patch_data(self, data, token , endpoint):
        return self._http_patch_authenticated(endpoint, payload=data, token=token)


    def get_data(self, token, endpoint):
        headers = {'Authorization': 'Bearer ' + token}
        return self._http_get(endpoint, headers)


    def publish_data(self, data, recipient, token, endpoint):
        """
        Make data available to a specific data owner.

        Parameters
        ----------
        data: dict
        recipient: str
            user_id
        token: str
            JWT
        endpoint: str
            API endpoint
        """
        user_name = 'owner_' + recipient
        return self._http_post_authenticated(endpoint, payload=data, token=token)
