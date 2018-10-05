
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
                'user_register': '/rpc/user_register',
                'user_delete': '/rpc/user_delete'
            }
        else:
            self.api_endpoints = api_endpoints
        self.funcs = {
            'user_register': self.user_register,
            'user_delete': self.user_delete
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
        return func(endpoint, data, token)

    # helper functions

    def _assert_keys_present(self, required_keys, existing_keys):
        try:
            for rk in required_keys:
                assert rk in existing_keys
        except AssertionError:
            # make own exception
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
            endpoint = '/rpc/token?id=' + user_id + '&token_type=' + token_type
        else:
            endpoint = '/rpc/token?token_type=' + token_type
        resp = self._http_get(endpoint)
        return json.loads(resp.text)['token']


    # table functions

    def table_create(self, endpoint, data, token):
        pass


    def table_describe(self, table_name, table_description):
        pass


    def table_describe_columns(self, table_name, column_descriptions):
        pass


    def table_metadata(self, table_name):
        pass


    def table_group_access_grant(self, table_name, group_name):
        pass


    def table_group_access_revoke(self, table_name, group_name):
        pass

    # user functions

    def user_register(self, endpoint, data, token):
        self._assert_keys_present(['user_id', 'user_type', 'user_metadata'], data.keys())
        assert data['user_type'] in ['data_owner', 'data_user']
        return self._http_post_unauthenticated(endpoint, payload=data)


    def user_group_remove(self, group_name):
        pass


    def user_groups(self):
        pass


    def user_delete_data(self):
        pass


    def user_delete(self, endpoint, data, token):
        self._assert_keys_present(['user_name'], data.keys())
        return self._http_post_authenticated(endpoint, payload=data, token=token)

    # group functions

    def group_create(self, group_name, group_metadata):
        pass


    def group_add_members(self):
        pass


    def _group_add_members_members(self, group_name, members):
        pass


    def _group_add_members_metadata(self, group_name, group_metadata):
        pass


    def _group_add_members_all_owners(self, group_name):
        pass


    def _group_add_members_all_users(self, group_name):
        pass


    def _group_add_members_all(self, group_name):
        pass


    def group_list_members(self, group_name):
        pass


    def group_remove_members(self, group_name):
        pass


    def group_delete(self, group_name):
        pass

    # informational views, tables, and event logs

    def get_table_overview(self):
        pass


    def get_user_registrations(self):
        pass


    def get_groups(self):
        pass


    def get_event_log_user_group_removals(self):
        pass


    def get_event_log_user_data_deletions(self):
        pass


    def get_event_log_data_access(self):
        pass


    def get_event_log_access_control(self):
        pass

    # utility functions (not in the SQL API)

    def post_data(self, table, data):
        pass


    def get_data(self, table):
        pass

